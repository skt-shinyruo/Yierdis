package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation;

import java.util.Objects;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.TtlReadOps;
import yier.bubu.redis.storage.api.TtlWriteOps;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;

final class YierdisTtlOps implements TtlReadOps, TtlWriteOps {
    private final YierdisDbKernel kernel;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final YierdisDbMemoryContext memoryContext;

    YierdisTtlOps(
            YierdisDbKernel kernel,
            YierdisDbKeyLifecycle keyLifecycle,
            YierdisDbMemoryContext memoryContext
    ) {
        this.kernel = Objects.requireNonNull(kernel, "kernel");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.memoryContext = Objects.requireNonNull(memoryContext, "memoryContext");
    }

    @Override
    public WriteResult<Boolean> expire(BytesView keyView, long seconds) {
        kernel.checkOwner();
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
        kernel.checkOwner();
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
        kernel.checkOwner();
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
        kernel.checkOwner();
        KeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        EntryRecord record = liveRecord(handle);
        if (record == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }

        if (record.expireAtMillis() < 0L) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        return kernel.execute(new MutationUse<WriteResult<Boolean>>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public Admission admission() {
                return Admission.RECLAMATION;
            }

            @Override
            public PreparedDbMutation<WriteResult<Boolean>> prepare(YierdisDbKernel scope) {
                EntryHandle entryHandle = keyLifecycle.entryHandle(keyLifecycle.copyKeyBytes(handle));
                EntryRecord current = entryHandle == null ? null : keyLifecycle.entryRecord(entryHandle);
                if (current == null || !record.equals(current) || current.expireAtMillis() < 0L) {
                    return preparedNoEntry(scope, WriteResult.unchanged(Boolean.FALSE), MutationOutcome.NONE);
                }
                EntryRecord next = keyLifecycle.withExpireAtMillis(handle, current, -1L);
                WriteResult<Boolean> result = WriteResult.of(Boolean.TRUE, MutationOutcome.TTL_CHANGED);
                return scope.replace(
                        result,
                        0L,
                        0L,
                        MutationOutcome.TTL_CHANGED,
                        entryHandle,
                        current,
                        next,
                        false
                );
            }
        });
    }

    @Override
    public long ttlSeconds(BytesView keyView) {
        return kernel.read(scope -> {
            KeyHandle handle = keyLifecycle.keyHandle(keyView);
            if (handle == null) {
                return -2L;
            }
            EntryRecord record = scope.liveEntryRecord(handle);
            if (record == null) {
                return -2L;
            }
            keyLifecycle.touchRecord(handle, record);

            long now = System.currentTimeMillis();
            long expireAtMillis = record.expireAtMillis();
            if (expireAtMillis < 0L) {
                return -1L;
            }
            long remainingMillis = expireAtMillis - now;
            return remainingMillis <= 0 ? -2L : remainingMillis / 1000L;
        });
    }

    @Override
    public long ttlMillis(BytesView keyView) {
        return kernel.read(scope -> {
            KeyHandle handle = keyLifecycle.keyHandle(keyView);
            if (handle == null) {
                return -2L;
            }
            EntryRecord record = scope.liveEntryRecord(handle);
            if (record == null) {
                return -2L;
            }
            keyLifecycle.touchRecord(handle, record);

            long now = System.currentTimeMillis();
            long expireAtMillis = record.expireAtMillis();
            if (expireAtMillis < 0L) {
                return -1L;
            }
            long remainingMillis = expireAtMillis - now;
            return remainingMillis <= 0 ? -2L : remainingMillis;
        });
    }

    private WriteResult<Boolean> deleteImmediately(
            KeyHandle handle,
            EntryRecord record
    ) {
        return kernel.execute(new MutationUse<WriteResult<Boolean>>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public Admission admission() {
                return Admission.RECLAMATION;
            }

            @Override
            public PreparedDbMutation<WriteResult<Boolean>> prepare(YierdisDbKernel scope) {
                EntryHandle entryHandle = keyLifecycle.entryHandle(keyLifecycle.copyKeyBytes(handle));
                EntryRecord current = entryHandle == null ? null : keyLifecycle.entryRecord(entryHandle);
                if (current == null || !record.equals(current)) {
                    return preparedNoEntry(scope, WriteResult.unchanged(Boolean.FALSE), MutationOutcome.NONE);
                }
                return scope.delete(
                        WriteResult.of(Boolean.TRUE, MutationOutcome.VALUE_CHANGED),
                        -keyLifecycle.estimatedBytesForRemoval(handle, current),
                        MutationOutcome.VALUE_CHANGED,
                        entryHandle,
                        current,
                        true
                );
            }
        });
    }

    private WriteResult<Boolean> setExpirePrepared(
            KeyHandle handle,
            EntryRecord record,
            long expireAtMillis
    ) {
        long upperBound = memoryContext.nativeAllocationScopeBookkeepingBytes(0);
        return kernel.execute(new MutationUse<WriteResult<Boolean>>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public PreparedDbMutation<WriteResult<Boolean>> prepare(YierdisDbKernel scope) {
                EntryHandle entryHandle = keyLifecycle.entryHandle(keyLifecycle.copyKeyBytes(handle));
                EntryRecord current = entryHandle == null ? null : keyLifecycle.entryRecord(entryHandle);
                if (current == null || !record.equals(current)) {
                    return preparedNoEntry(scope, WriteResult.unchanged(Boolean.FALSE), MutationOutcome.NONE);
                }
                EntryRecord next = keyLifecycle.withExpireAtMillis(handle, current, expireAtMillis);
                WriteResult<Boolean> result = WriteResult.of(Boolean.TRUE, MutationOutcome.TTL_CHANGED);
                return scope.replace(
                        result,
                        0L,
                        0L,
                        MutationOutcome.TTL_CHANGED,
                        entryHandle,
                        current,
                        next,
                        false
                );
            }
        });
    }

    private static <T> PreparedDbMutation<T> preparedNoEntry(
            YierdisDbKernel scope,
            T result,
            MutationOutcome outcome
    ) {
        return scope.unchanged(result, outcome);
    }

    private EntryRecord liveRecord(KeyHandle handle) {
        EntryRecord record = keyLifecycle.entryRecord(handle);
        if (record == null) {
            return null;
        }
        long nowMillis = System.currentTimeMillis();
        if (record.expireAtMillis() >= 0L && record.expireAtMillis() <= nowMillis) {
            kernel.reclaimExpired(handle, record, nowMillis);
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

}
