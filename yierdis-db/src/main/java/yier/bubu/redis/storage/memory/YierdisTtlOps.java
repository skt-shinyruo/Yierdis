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
import yier.bubu.redis.storage.api.YierdisCommandException;
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
        // Redis expireGenericCommand 在查键之前拒绝秒换算和 now+delta 溢出。
        // 缺失键、已过期键因此仍回报错，而不是 :0；也不能把 deadline 钳到 Long.MAX_VALUE。
        long expireAtMillis = relativeExpireAtMillis(System.currentTimeMillis(), seconds, "expire");
        AllocatorKeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        EntryRecord record = liveRecord(handle);
        if (record == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
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
        // PEXPIRE 没有秒换算，只在查键前拒绝 now+delta 溢出。绝对毫秒时间戳不走这条路径。
        long expireAtMillis = addExpireBase(System.currentTimeMillis(), milliseconds, "pexpire");
        AllocatorKeyHandle handle = keyLifecycle.keyHandle(keyView);
        if (handle == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }
        EntryRecord record = liveRecord(handle);
        if (record == null) {
            return WriteResult.unchanged(Boolean.FALSE);
        }

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
        // basetime 为 0，只检查秒×1000。溢出必须报 'expireat'，不能先钳位再交给 expireAtMillis。
        return expireAtMillis(keyView, secondsToMillis(unixSeconds, "expireat"), condition);
    }

    @Override
    public WriteResult<Boolean> expireAtMillis(BytesView keyView, long unixMillis, ExpireCondition condition) {
        kernel.checkOwner();
        // PEXPIREAT 的参数已是绝对毫秒。long 能表示的值，包括 Long.MAX_VALUE，都不再做溢出判断。
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

    // 秒范围对齐 Redis 的 [Long.MIN_VALUE/1000, Long.MAX_VALUE/1000]，然后再做 when > Long.MAX_VALUE - now。
    // nowMillis 由命令路径传入 System.currentTimeMillis()。减法不回绕、负 delta 不落到 Long.MIN_VALUE 以下，
    // 都以 nowMillis >= 0 为前提；这个方法本身不拒绝负的 now。
    private static long relativeExpireAtMillis(long nowMillis, long seconds, String command) {
        return addExpireBase(nowMillis, secondsToMillis(seconds, command), command);
    }

    // 向零截断下，能安全乘 1000 的秒范围是 [Long.MIN_VALUE/1000, Long.MAX_VALUE/1000]。
    private static long secondsToMillis(long seconds, String command) {
        if (seconds > Long.MAX_VALUE / 1000L || seconds < Long.MIN_VALUE / 1000L) {
            throw invalidExpireTime(command);
        }
        return seconds * 1000L;
    }

    // 比较式对应 Redis 的 when > LLONG_MAX - basetime。nowMillis >= 0 时减法不会回绕。
    private static long addExpireBase(long nowMillis, long deltaMillis, String command) {
        if (deltaMillis > Long.MAX_VALUE - nowMillis) {
            throw invalidExpireTime(command);
        }
        return nowMillis + deltaMillis;
    }

    private static YierdisCommandException invalidExpireTime(String command) {
        return new YierdisCommandException("ERR invalid expire time in '" + command + "' command");
    }

}
