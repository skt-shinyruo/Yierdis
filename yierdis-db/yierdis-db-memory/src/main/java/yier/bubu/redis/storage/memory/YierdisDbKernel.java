package yier.bubu.redis.storage.memory;

import yier.bubu.redis.common.command.ByteArrayCommandRecord;
import yier.bubu.redis.common.command.ImmutableCommandRecord;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.storage.api.DbCommitKind;
import yier.bubu.redis.storage.api.DbCommitStreamUnavailableException;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;

import java.util.Objects;

final class YierdisDbKernel {
    private static final byte[] DELETE_COMMAND = new byte[]{'D', 'E', 'L'};
    private final Runnable threadChecker;
    private final YierdisDbMutationExecutor mutationExecutor;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final ReadScope readScope;
    private final MutationScope mutationScope;
    private final InspectionScope inspectionScope;
    private final MaintenanceScope maintenanceScope;

    YierdisDbKernel(
            Runnable threadChecker,
            YierdisDbMutationExecutor mutationExecutor,
            YierdisDbKeyLifecycle keyLifecycle,
            YierdisDbMemoryContext memoryContext
    ) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.mutationExecutor = Objects.requireNonNull(mutationExecutor, "mutationExecutor");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        YierdisDbMemoryContext checkedMemoryContext = Objects.requireNonNull(memoryContext, "memoryContext");
        this.readScope = new ReadScope(this);
        this.mutationScope = new MutationScope(keyLifecycle);
        this.inspectionScope = new InspectionScope(this, keyLifecycle, checkedMemoryContext);
        this.maintenanceScope = new MaintenanceScope(this, keyLifecycle, checkedMemoryContext);
    }

    <R> R execute(DbUse<R> use) {
        Objects.requireNonNull(use, "use");
        if (use instanceof MutationUse<?> mutationUse) {
            return executeMutationUse(castMutationUse(mutationUse));
        }
        threadChecker.run();
        if (use instanceof ReadUse<?> readUse) {
            return YierdisDbKernel.<R>castReadUse(readUse).execute(readScope);
        }
        if (use instanceof InspectionUse<?> inspectionUse) {
            return YierdisDbKernel.<R>castInspectionUse(inspectionUse).execute(inspectionScope);
        }
        if (use instanceof MaintenanceUse<?> maintenanceUse) {
            return YierdisDbKernel.<R>castMaintenanceUse(maintenanceUse).execute(maintenanceScope);
        }
        throw new IllegalArgumentException("unsupported DB use: " + use.getClass().getName());
    }

    void bindToCurrentThread() {
        keyLifecycle.bindToCurrentThread();
    }

    void close() {
        keyLifecycle.close();
    }

    private <R> R executeMutationUse(MutationUse<R> use) {
        MutationContext context = Objects.requireNonNull(use.context(), "mutation context");
        CommitSpec commit = Objects.requireNonNull(use.commit(), "commit spec");
        return mutationExecutor.execute(context, new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return use.upperBoundBytes();
            }

            @Override
            public AdmissionMode admissionMode() {
                return use.admission() == Admission.RECLAMATION
                        ? AdmissionMode.RECLAMATION
                        : AdmissionMode.NORMAL;
            }

            @Override
            public DbCommitKind commitKind() {
                return commit.kind();
            }

            @Override
            public boolean requiresCommitStream() {
                return commit.required();
            }

            @Override
            public ImmutableCommandRecord retainCommitRecord(MutationContext mutationContext) {
                return commit.retainRecord(mutationContext);
            }

            @Override
            public yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation<R> prepare() {
                PreparedChange<R> prepared = Objects.requireNonNull(
                        use.prepare(mutationScope),
                        "prepared change"
                );
                return prepared.unwrap();
            }
        });
    }

    @SuppressWarnings("unchecked")
    private static <R> MutationUse<R> castMutationUse(MutationUse<?> use) {
        return (MutationUse<R>) use;
    }

    @SuppressWarnings("unchecked")
    private static <R> ReadUse<R> castReadUse(ReadUse<?> use) {
        return (ReadUse<R>) use;
    }

    @SuppressWarnings("unchecked")
    private static <R> InspectionUse<R> castInspectionUse(InspectionUse<?> use) {
        return (InspectionUse<R>) use;
    }

    @SuppressWarnings("unchecked")
    private static <R> MaintenanceUse<R> castMaintenanceUse(MaintenanceUse<?> use) {
        return (MaintenanceUse<R>) use;
    }

    EntryRecord liveEntryRecord(KeyHandle keyHandle) {
        if (keyHandle == null) {
            return null;
        }
        EntryRecord record = keyLifecycle.entryRecord(keyHandle);
        if (record == null) {
            return null;
        }
        long nowMillis = System.currentTimeMillis();
        if (!keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return record;
        }
        reclaimExpired(keyHandle, record, nowMillis);
        return null;
    }

    boolean reclaimExpired(KeyHandle keyHandle, EntryRecord expectedRecord, long nowMillis) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        ReclamationAttempt attempt = new ReclamationAttempt();
        if (!keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return false;
        }
        try {
            return reclaim(keyHandle, expectedRecord, DbCommitKind.EXPIRED, nowMillis, true, attempt);
        } catch (DbCommitStreamUnavailableException unavailable) {
            if (attempt.entryHandle == null) {
                attempt.track(
                        keyLifecycle.entryHandle(keyLifecycle.copyKeyBytes(keyHandle)),
                        expectedRecord
                );
            }
            keyLifecycle.markExpiredEntryAwaitingPhysicalDeletion(
                    keyHandle,
                    attempt.entryHandle,
                    attempt.record,
                    nowMillis
            );
            return false;
        }
    }

    boolean evict(KeyHandle keyHandle, EntryRecord expectedRecord) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        return reclaim(keyHandle, expectedRecord, DbCommitKind.EVICTED, 0L, false, null);
    }

    void reclaimExpiredBeforeMutation(byte[] keyBytes, long nowMillis) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        if (keyHandle == null) {
            return;
        }
        EntryRecord record = keyLifecycle.entryRecord(keyHandle);
        if (record != null && keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            reclaimExpired(keyHandle, record, nowMillis);
        }
    }

    private boolean reclaim(
            KeyHandle keyHandle,
            EntryRecord expectedRecord,
            DbCommitKind commitKind,
            long nowMillis,
            boolean requireExpired,
            ReclamationAttempt attempt
    ) {
        return execute(new MutationUse<Boolean>() {
            private byte[] deletedKey;

            @Override
            public long upperBoundBytes() {
                return 0L;
            }

            @Override
            public Admission admission() {
                return Admission.RECLAMATION;
            }

            @Override
            public CommitSpec commit() {
                return CommitSpec.synthetic(commitKind, () -> {
                    if (deletedKey == null) {
                        throw new IllegalStateException("synthetic deletion record is unavailable");
                    }
                    return ByteArrayCommandRecord.copyOf(DELETE_COMMAND, deletedKey);
                });
            }

            @Override
            public PreparedChange<Boolean> prepare(MutationScope scope) {
                EntryRecord current = keyLifecycle.entryRecord(keyHandle);
                if (current == null || (expectedRecord != null && !expectedRecord.equals(current))) {
                    return preparedNoDeletion();
                }
                if (requireExpired && !keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
                    return preparedNoDeletion();
                }
                byte[] keyBytes = keyLifecycle.copyKeyBytes(keyHandle);
                EntryHandle entryHandle = keyLifecycle.entryHandle(keyBytes);
                if (entryHandle == null) {
                    return preparedNoDeletion();
                }
                if (attempt != null) {
                    attempt.track(entryHandle, current);
                }
                long removalBytes = keyLifecycle.estimatedBytesForRemoval(keyHandle, current);
                deletedKey = keyBytes;
                return scope.delete(
                        Boolean.TRUE,
                        -removalBytes,
                        MutationOutcome.VALUE_CHANGED,
                        entryHandle,
                        current,
                        true
                );
            }

            private PreparedChange<Boolean> preparedNoDeletion() {
                return mutationScope.unchanged(Boolean.FALSE, MutationOutcome.NONE);
            }
        });
    }

    private static final class ReclamationAttempt {
        private EntryHandle entryHandle;
        private EntryRecord record;

        private void track(EntryHandle entryHandle, EntryRecord record) {
            this.entryHandle = entryHandle;
            this.record = record;
        }
    }
}
