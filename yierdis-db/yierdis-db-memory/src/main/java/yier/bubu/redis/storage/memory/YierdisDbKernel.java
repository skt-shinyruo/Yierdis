package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.CurrentEntry;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.StagedEntry;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedCallbackMutation;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;

import java.util.Objects;

final class YierdisDbKernel {
    private final Runnable threadChecker;
    private final YierdisDbMutationExecutor mutationExecutor;
    private final YierdisDbKeyLifecycle keyLifecycle;

    YierdisDbKernel(
            Runnable threadChecker,
            YierdisDbMutationExecutor mutationExecutor,
            YierdisDbKeyLifecycle keyLifecycle
    ) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.mutationExecutor = Objects.requireNonNull(mutationExecutor, "mutationExecutor");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
    }

    <R> R execute(MutationUse<R> use) {
        Objects.requireNonNull(use, "use");
        return executeMutationUse(use);
    }

    void checkOwner() {
        threadChecker.run();
    }

    void bindToCurrentThread() {
        keyLifecycle.bindToCurrentThread();
    }

    void close() {
        keyLifecycle.close();
    }

    private <R> R executeMutationUse(MutationUse<R> use) {
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<>() {
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
            public PreparedDbMutation<R> prepare() {
                return Objects.requireNonNull(
                        use.prepare(YierdisDbKernel.this),
                        "prepared change"
                );
            }
        });
    }

    <T> PreparedEntryMutation<T> unchanged(T result) {
        return PreparedEntryMutation.unchanged(keyLifecycle, result);
    }

    <T> PreparedEntryMutation<T> insert(
            T result,
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            StagedEntry stagedEntry,
            EntryRecord newRecord
    ) {
        return PreparedEntryMutation.insert(
                keyLifecycle,
                result,
                actualDeltaBytes,
                stagedNonNativeGrowthBytes,
                stagedEntry,
                newRecord
        );
    }

    <T> PreparedEntryMutation<T> replace(
            T result,
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            EntryHandle existingEntryHandle,
            EntryRecord oldRecord,
            EntryRecord newRecord,
            boolean releaseReplacedValue
    ) {
        return PreparedEntryMutation.replace(
                keyLifecycle,
                result,
                actualDeltaBytes,
                stagedNonNativeGrowthBytes,
                existingEntryHandle,
                oldRecord,
                newRecord,
                releaseReplacedValue
        );
    }

    <T> PreparedEntryMutation<T> delete(
            T result,
            long actualDeltaBytes,
            EntryHandle existingEntryHandle,
            EntryRecord oldRecord,
            boolean releaseReplacedValue
    ) {
        return PreparedEntryMutation.delete(
                keyLifecycle,
                result,
                actualDeltaBytes,
                existingEntryHandle,
                oldRecord,
                releaseReplacedValue
        );
    }

    <T> PreparedEntryMutation<T> upsert(
            T result,
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            CurrentEntry current,
            StagedEntry staged,
            EntryRecord newRecord,
            boolean releaseReplacedValue
    ) {
        return PreparedEntryMutation.upsert(
                keyLifecycle,
                result,
                actualDeltaBytes,
                stagedNonNativeGrowthBytes,
                current,
                staged,
                newRecord,
                releaseReplacedValue
        );
    }

    <T> PreparedDbMutation<T> callback(
            T result,
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            Runnable commit,
            Runnable releaseSuperseded,
            Runnable abort,
            boolean trimNativePagesAfterCommit
    ) {
        return new PreparedCallbackMutation<>(
                result,
                actualDeltaBytes,
                stagedNonNativeGrowthBytes,
                commit,
                releaseSuperseded,
                abort,
                trimNativePagesAfterCommit
        );
    }

    <T> PreparedDbMutation<T> batch(
            PreparedDbMutation<?>[] changes,
            int count,
            T result,
            long actualDeltaBytes
    ) {
        return new PreparedBatchMutation<>(changes, count, result, actualDeltaBytes);
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
        if (!keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return false;
        }
        return reclaim(keyHandle, expectedRecord, nowMillis, true);
    }

    boolean evict(KeyHandle keyHandle, EntryRecord expectedRecord) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        return reclaim(keyHandle, expectedRecord, 0L, false);
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
            long nowMillis,
            boolean requireExpired
    ) {
        return execute(new MutationUse<Boolean>() {
            @Override
            public long upperBoundBytes() {
                return 0L;
            }

            @Override
            public Admission admission() {
                return Admission.RECLAMATION;
            }

            @Override
            public PreparedDbMutation<Boolean> prepare(YierdisDbKernel kernel) {
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
                long removalBytes = keyLifecycle.estimatedBytesForRemoval(keyHandle, current);
                return kernel.delete(
                        Boolean.TRUE,
                        -removalBytes,
                        entryHandle,
                        current,
                        true
                );
            }

            private PreparedDbMutation<Boolean> preparedNoDeletion() {
                return unchanged(Boolean.FALSE);
            }
        });
    }

}
