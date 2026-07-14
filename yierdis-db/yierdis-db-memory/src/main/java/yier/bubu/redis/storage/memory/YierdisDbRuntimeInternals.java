package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.ledger.MemoryLedger;
import yier.bubu.redis.common.command.ByteArrayCommandRecord;
import yier.bubu.redis.common.command.ImmutableCommandRecord;
import yier.bubu.redis.storage.api.DbCommitKind;
import yier.bubu.redis.storage.api.DbCommitStreamUnavailableException;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.expire.PreparedTtlMutation;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedEntryMutation;

import java.util.Objects;

public final class YierdisDbRuntimeInternals implements YierdisDbInternals {
    private static final byte[] DELETE_COMMAND = new byte[]{'D', 'E', 'L'};
    private final Runnable threadChecker;
    private final YierdisDbMutationExecutor mutationExecutor;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final MemoryLedger ledger;

    YierdisDbRuntimeInternals(
            Runnable threadChecker,
            YierdisDbMutationExecutor mutationExecutor,
            YierdisDbKeyLifecycle keyLifecycle,
            MemoryLedger ledger
    ) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.mutationExecutor = Objects.requireNonNull(mutationExecutor, "mutationExecutor");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.ledger = Objects.requireNonNull(ledger, "ledger");
    }

    @Override
    public void checkThread() {
        threadChecker.run();
    }

    @Override
    public <T> T executeMutation(YierdisDbMutationExecutor.MutationPlan<T> plan) {
        return mutationExecutor.execute(plan);
    }

    @Override
    public EntryRecord liveEntryRecord(KeyHandle keyHandle) {
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

    @Override
    public boolean reclaimExpired(KeyHandle keyHandle, EntryRecord expectedRecord, long nowMillis) {
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

    @Override
    public boolean evict(KeyHandle keyHandle, EntryRecord expectedRecord) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        return reclaim(keyHandle, expectedRecord, DbCommitKind.EVICTED, 0L, false, null);
    }

    @Override
    public YierdisDbKeyLifecycle keyLifecycle() {
        return keyLifecycle;
    }

    @Override
    public MemoryLedger ledger() {
        return ledger;
    }

    private boolean reclaim(
            KeyHandle keyHandle,
            EntryRecord expectedRecord,
            DbCommitKind commitKind,
            long nowMillis,
            boolean requireExpired,
            ReclamationAttempt attempt
    ) {
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<>() {
            private byte[] deletedKey;

            @Override
            public long upperBoundBytes() {
                return 0L;
            }

            @Override
            public AdmissionMode admissionMode() {
                return AdmissionMode.RECLAMATION;
            }

            @Override
            public DbCommitKind commitKind() {
                return commitKind;
            }

            @Override
            public ImmutableCommandRecord retainCommitRecord() {
                if (deletedKey == null) {
                    throw new IllegalStateException("synthetic deletion record is unavailable");
                }
                return ByteArrayCommandRecord.copyOf(DELETE_COMMAND, deletedKey);
            }

            @Override
            public PreparedEntryMutation<Boolean> prepare() {
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
                PreparedTtlMutation ttlMutation = keyLifecycle.prepareRemoveExpire(keyHandle);
                deletedKey = keyBytes;
                return new PreparedEntryMutation<>(
                        keyLifecycle,
                        Boolean.TRUE,
                        -removalBytes,
                        0L,
                        MutationOutcome.VALUE_CHANGED,
                        entryHandle,
                        null,
                        null,
                        current,
                        null,
                        true,
                        ttlMutation
                );
            }

            private PreparedEntryMutation<Boolean> preparedNoDeletion() {
                return new PreparedEntryMutation<>(
                        keyLifecycle,
                        Boolean.FALSE,
                        0L,
                        0L,
                        MutationOutcome.NONE,
                        null,
                        null,
                        null,
                        null,
                        null,
                        false,
                        PreparedTtlMutation.NONE
                );
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
