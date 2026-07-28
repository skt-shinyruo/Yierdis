package yier.bubu.redis.storage.memory.internal.expire;

import java.util.Objects;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.TtlReadOps;
import yier.bubu.redis.storage.api.TtlWriteOps;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.memory.YierdisDbInternals;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedEntryMutation;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;

public final class YierdisTtlOps implements TtlReadOps, TtlWriteOps {
    private final YierdisDbInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;

    public YierdisTtlOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = internals.keyLifecycle();
    }

    @Override
    public WriteResult<Boolean> expire(BytesView keyView, long seconds) {
        internals.checkThread();
        KeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        EntryRecord record = liveRecord(handle);
        if (record == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        if (seconds <= 0) {
            return deleteImmediately(handle, record);
        }

        long expireAtMillis = safeExpireAtMillis(System.currentTimeMillis(), seconds);
        return setExpirePrepared(handle, record, expireAtMillis);
    }

    @Override
    public WriteResult<Boolean> pexpire(BytesView keyView, long milliseconds) {
        internals.checkThread();
        KeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        EntryRecord record = liveRecord(handle);
        if (record == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }

        if (milliseconds <= 0) {
            return deleteImmediately(handle, record);
        }

        long expireAtMillis = safeAddMillis(System.currentTimeMillis(), milliseconds);
        return setExpirePrepared(handle, record, expireAtMillis);
    }

    @Override
    public WriteResult<Boolean> expireAtSeconds(BytesView keyView, long unixSeconds) {
        long expireAtMillis;
        try {
            expireAtMillis = Math.multiplyExact(unixSeconds, 1000L);
        } catch (ArithmeticException e) {
            expireAtMillis = Long.MAX_VALUE;
        }
        return expireAtMillis(keyView, expireAtMillis);
    }

    @Override
    public WriteResult<Boolean> expireAtMillis(BytesView keyView, long unixMillis) {
        internals.checkThread();
        KeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        EntryRecord record = liveRecord(handle);
        long now = System.currentTimeMillis();
        if (record == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }

        if (unixMillis <= now) {
            return deleteImmediately(handle, record);
        }

        return setExpirePrepared(handle, record, unixMillis);
    }

    @Override
    public WriteResult<Boolean> persist(BytesView keyView) {
        internals.checkThread();
        KeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        EntryRecord record = liveRecord(handle);
        if (record == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }

        Long expireAtMillis = keyLifecycle.expireAtMillis(handle);
        if (expireAtMillis == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Boolean>>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public AdmissionMode admissionMode() {
                return AdmissionMode.RECLAMATION;
            }

            @Override
            public PreparedEntryMutation<WriteResult<Boolean>> prepare() {
                EntryHandle entryHandle = keyLifecycle.entryHandle(keyLifecycle.copyKeyBytes(handle));
                EntryRecord current = entryHandle == null ? null : keyLifecycle.entryRecord(entryHandle);
                if (current == null) {
                    return preparedNoEntry(WriteResult.unchanged(Boolean.FALSE), MutationOutcome.NONE);
                }
                PreparedTtlMutation ttlMutation = keyLifecycle.prepareRemoveExpire(handle);
                EntryRecord next = keyLifecycle.withExpireAtMillis(handle, current, -1L);
                WriteResult<Boolean> result = WriteResult.of(Boolean.TRUE, MutationOutcome.TTL_CHANGED);
                return PreparedEntryMutation.replace(
                        keyLifecycle,
                        result,
                        0L,
                        0L,
                        MutationOutcome.TTL_CHANGED,
                        entryHandle,
                        current,
                        next,
                        false,
                        ttlMutation
                );
            }
        });
    }

    @Override
    public long ttlSeconds(BytesView keyView) {
        internals.checkThread();
        KeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return -2;
        }
        EntryRecord record = liveRecord(handle);
        if (record == null) {
            return -2;
        }
        keyLifecycle.touchRecord(handle, record);

        long now = System.currentTimeMillis();
        Long expireAtMillis = keyLifecycle.expireAtMillis(handle);
        if (expireAtMillis == null) {
            return -1;
        }
        long remainingMillis = expireAtMillis - now;
        return remainingMillis <= 0 ? -2 : remainingMillis / 1000L;
    }

    @Override
    public long ttlMillis(BytesView keyView) {
        internals.checkThread();
        KeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return -2;
        }
        EntryRecord record = liveRecord(handle);
        if (record == null) {
            return -2;
        }
        keyLifecycle.touchRecord(handle, record);

        long now = System.currentTimeMillis();
        Long expireAtMillis = keyLifecycle.expireAtMillis(handle);
        if (expireAtMillis == null) {
            return -1;
        }
        long remainingMillis = expireAtMillis - now;
        return remainingMillis <= 0 ? -2 : remainingMillis;
    }

    private WriteResult<Boolean> deleteImmediately(KeyHandle handle, EntryRecord record) {
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public AdmissionMode admissionMode() {
                return AdmissionMode.RECLAMATION;
            }

            @Override
            public PreparedEntryMutation<WriteResult<Boolean>> prepare() {
                EntryHandle entryHandle = keyLifecycle.entryHandle(keyLifecycle.copyKeyBytes(handle));
                EntryRecord current = entryHandle == null ? null : keyLifecycle.entryRecord(entryHandle);
                if (current == null || !record.equals(current)) {
                    return preparedNoEntry(WriteResult.unchanged(Boolean.FALSE), MutationOutcome.NONE);
                }
                PreparedTtlMutation ttlMutation = PreparedTtlMutation.NONE;
                try {
                    ttlMutation = keyLifecycle.prepareRemoveExpire(handle);
                    return PreparedEntryMutation.delete(
                            keyLifecycle,
                            WriteResult.of(Boolean.TRUE, MutationOutcome.VALUE_CHANGED),
                            -keyLifecycle.estimatedBytesForRemoval(handle, current),
                            MutationOutcome.VALUE_CHANGED,
                            entryHandle,
                            current,
                            true,
                            ttlMutation
                    );
                } catch (RuntimeException | Error failure) {
                    try {
                        ttlMutation.abort();
                    } catch (RuntimeException | Error abortFailure) {
                        failure.addSuppressed(abortFailure);
                    }
                    throw failure;
                }
            }
        });
    }

    private WriteResult<Boolean> setExpirePrepared(KeyHandle handle, EntryRecord record, long expireAtMillis) {
        long upperBound = upperBoundForSetExpire(handle);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<WriteResult<Boolean>>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public PreparedEntryMutation<WriteResult<Boolean>> prepare() {
                EntryHandle entryHandle = keyLifecycle.entryHandle(keyLifecycle.copyKeyBytes(handle));
                EntryRecord current = entryHandle == null ? null : keyLifecycle.entryRecord(entryHandle);
                if (current == null) {
                    return preparedNoEntry(WriteResult.unchanged(Boolean.FALSE), MutationOutcome.NONE);
                }
                PreparedTtlMutation ttlMutation = keyLifecycle.prepareSetExpireAtMillis(handle, expireAtMillis);
                EntryRecord next = keyLifecycle.withExpireAtMillis(handle, current, expireAtMillis);
                WriteResult<Boolean> result = WriteResult.of(Boolean.TRUE, MutationOutcome.TTL_CHANGED);
                return PreparedEntryMutation.replace(
                        keyLifecycle,
                        result,
                        0L,
                        0L,
                        MutationOutcome.TTL_CHANGED,
                        entryHandle,
                        current,
                        next,
                        false,
                        ttlMutation
                );
            }
        });
    }

    private <T> PreparedEntryMutation<T> preparedNoEntry(T result, MutationOutcome outcome) {
        return PreparedEntryMutation.unchanged(keyLifecycle, result, outcome);
    }

    private EntryRecord liveRecord(KeyHandle handle) {
        EntryRecord record = keyLifecycle.entryRecord(handle);
        if (record == null) {
            return null;
        }
        long nowMillis = System.currentTimeMillis();
        if (keyLifecycle.isKeyExpired(handle, nowMillis)) {
            internals.reclaimExpired(handle, record, nowMillis);
            return null;
        }
        return record;
    }

    private static long safeExpireAtMillis(long nowMillis, long seconds) {
        long deltaMillis;
        try {
            deltaMillis = Math.multiplyExact(seconds, 1000L);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
        try {
            return Math.addExact(nowMillis, deltaMillis);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static long safeAddMillis(long nowMillis, long deltaMillis) {
        try {
            return Math.addExact(nowMillis, deltaMillis);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static long ttlEntryBytesEstimate() {
        return yier.bubu.redis.storage.api.DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
    }

    private long upperBoundForSetExpire(KeyHandle handle) {
        boolean addingNewTtl = keyLifecycle.expireAtMillis(handle) == null;
        long logicalGrowth = addingNewTtl ? ttlEntryBytesEstimate() : 0L;
        return addSaturating(logicalGrowth, keyLifecycle.estimateExpireSetUpperBound(handle, addingNewTtl));
    }

    private static long addSaturating(long left, long right) {
        if (right <= 0L) {
            return left;
        }
        if (Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

}
