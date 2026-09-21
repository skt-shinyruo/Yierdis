package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor.MutationPlan;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor.MutationPlan.AdmissionMode;

import java.util.Objects;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.api.ExpireCondition;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.TtlOps;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.key.AllocatorKeyHandle;

final class YierdisTtlOps implements TtlOps {
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
    public WriteResult<Boolean> expire(BytesView keyView, long seconds, ExpireCondition condition) {
        kernel.checkOwner();
        AllocatorKeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        EntryRecord record = liveRecord(handle);
        if (record == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        long expireAtMillis = safeExpireAtMillis(System.currentTimeMillis(), seconds);
        // Redis 在 checkAlreadyExpired 删除分支之前判定条件：条件不满足时键与旧 TTL 都保留。
        if (!condition.allows(record.expireAtMillis(), expireAtMillis)) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        if (seconds <= 0) {
            return deleteImmediately(handle, record);
        }
        return setExpirePrepared(handle, record, expireAtMillis);
    }

    @Override
    public WriteResult<Boolean> pexpire(BytesView keyView, long milliseconds, ExpireCondition condition) {
        kernel.checkOwner();
        AllocatorKeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        EntryRecord record = liveRecord(handle);
        if (record == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }

        long expireAtMillis = safeAddMillis(System.currentTimeMillis(), milliseconds);
        if (!condition.allows(record.expireAtMillis(), expireAtMillis)) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        if (milliseconds <= 0) {
            return deleteImmediately(handle, record);
        }
        return setExpirePrepared(handle, record, expireAtMillis);
    }

    @Override
    public WriteResult<Boolean> expireAtSeconds(BytesView keyView, long unixSeconds, ExpireCondition condition) {
        long expireAtMillis;
        try {
            expireAtMillis = Math.multiplyExact(unixSeconds, 1000L);
        } catch (ArithmeticException e) {
            expireAtMillis = Long.MAX_VALUE;
        }
        return expireAtMillis(keyView, expireAtMillis, condition);
    }

    @Override
    public WriteResult<Boolean> expireAtMillis(BytesView keyView, long unixMillis, ExpireCondition condition) {
        kernel.checkOwner();
        AllocatorKeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        EntryRecord record = liveRecord(handle);
        long now = System.currentTimeMillis();
        if (record == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }

        if (!condition.allows(record.expireAtMillis(), unixMillis)) {
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
        AllocatorKeyHandle handle = keyLifecycle.keyHandle(keyView);
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
        return kernel.execute(new MutationPlan<WriteResult<Boolean>>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public AdmissionMode admissionMode() {
                return AdmissionMode.RECLAMATION;
            }

            @Override
            public PreparedDbMutation<WriteResult<Boolean>> prepare() {
                EntryHandle entryHandle = keyLifecycle.entryHandle(keyLifecycle.copyKeyBytes(handle));
                EntryRecord current = entryHandle == null ? null : keyLifecycle.entryRecord(entryHandle);
                if (current == null || !record.equals(current) || current.expireAtMillis() < 0L) {
                    return kernel.unchanged(WriteResult.unchanged(Boolean.FALSE));
                }
                EntryRecord next = keyLifecycle.withExpireAtMillis(handle, current, -1L);
                WriteResult<Boolean> result = WriteResult.of(Boolean.TRUE, MutationOutcome.TTL_CHANGED);
                return kernel.replace(
                        result,
                        0L,
                        0L,
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
        long remainingMillis = remainingTtlMillis(keyView);
        if (remainingMillis < 0L) {
            return remainingMillis;
        }
        // Redis 的 TTL 按 (剩余毫秒+500)/1000 四舍五入到秒；向下取整会让刚设置的 TTL 恒少 1 秒。
        return (remainingMillis + 500L) / 1000L;
    }

    @Override
    public long ttlMillis(BytesView keyView) {
        return remainingTtlMillis(keyView);
    }

    // 剩余毫秒；-1 表示 persistent；-2 表示 key 已不在（回答 -2 前先 reclaim，
    // 保证后续 GET/EXISTS 观察不到它；仍存在的 key 永远不会得到 -2）。
    private long remainingTtlMillis(BytesView keyView) {
        kernel.checkOwner();
        AllocatorKeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return -2L;
        }
        EntryRecord record = kernel.liveEntryRecord(handle);
        if (record == null) {
            return -2L;
        }
        // touch 在 LRU 下会写回新 record；后续 reclaim 必须以 touched record 校验 identity，
        // 否则 expectedRecord 与 current 的 lruOrLfu 不一致会让 reclamation 静默空转。
        record = keyLifecycle.touchRecord(handle, record);

        long now = System.currentTimeMillis();
        long expireAtMillis = record.expireAtMillis();
        if (expireAtMillis < 0L) {
            return -1L;
        }
        long remainingMillis = expireAtMillis - now;
        if (remainingMillis <= 0) {
            kernel.reclaimExpired(handle, record, now);
            return -2L;
        }
        return remainingMillis;
    }

    private WriteResult<Boolean> deleteImmediately(
            AllocatorKeyHandle handle,
            EntryRecord record
    ) {
        return kernel.execute(new MutationPlan<WriteResult<Boolean>>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public AdmissionMode admissionMode() {
                return AdmissionMode.RECLAMATION;
            }

            @Override
            public PreparedDbMutation<WriteResult<Boolean>> prepare() {
                EntryHandle entryHandle = keyLifecycle.entryHandle(keyLifecycle.copyKeyBytes(handle));
                EntryRecord current = entryHandle == null ? null : keyLifecycle.entryRecord(entryHandle);
                if (current == null || !record.equals(current)) {
                    return kernel.unchanged(WriteResult.unchanged(Boolean.FALSE));
                }
                return kernel.delete(
                        WriteResult.of(Boolean.TRUE, MutationOutcome.VALUE_CHANGED),
                        -keyLifecycle.estimatedBytesForRemoval(handle, current),
                        entryHandle,
                        current,
                        true
                );
            }
        });
    }

    private WriteResult<Boolean> setExpirePrepared(
            AllocatorKeyHandle handle,
            EntryRecord record,
            long expireAtMillis
    ) {
        long upperBound = memoryContext.nativeAllocationScopeBookkeepingBytes(0);
        return kernel.execute(new MutationPlan<WriteResult<Boolean>>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public PreparedDbMutation<WriteResult<Boolean>> prepare() {
                EntryHandle entryHandle = keyLifecycle.entryHandle(keyLifecycle.copyKeyBytes(handle));
                EntryRecord current = entryHandle == null ? null : keyLifecycle.entryRecord(entryHandle);
                if (current == null || !record.equals(current)) {
                    return kernel.unchanged(WriteResult.unchanged(Boolean.FALSE));
                }
                EntryRecord next = keyLifecycle.withExpireAtMillis(handle, current, expireAtMillis);
                WriteResult<Boolean> result = WriteResult.of(Boolean.TRUE, MutationOutcome.TTL_CHANGED);
                return kernel.replace(
                        result,
                        0L,
                        0L,
                        entryHandle,
                        current,
                        next,
                        false
                );
            }
        });
    }

    private EntryRecord liveRecord(AllocatorKeyHandle handle) {
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
