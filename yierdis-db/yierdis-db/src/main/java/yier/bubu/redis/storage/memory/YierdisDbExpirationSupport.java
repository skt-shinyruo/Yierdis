package yier.bubu.redis.storage.memory;

import java.util.Objects;
import yier.bubu.redis.memory.api.StaleNativeHandleException;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.keyspace.ExpiresIndex;

/**
 * 主动过期清理：只消费 expires 索引中已到期的候选，成本与到期候选（含队首 stale 项）
 * 的数量成正比，不再对 keyspace 做游标扫描。索引项可能 stale（re-SET、PERSIST、overwrite、
 * 删除重建），每个候选都先按 entry 的真实 expireAtMillis 与 key identity 惰性校验再决定回收或丢弃。
 */
final class YierdisDbExpirationSupport {
    private static final int CLEANUP_MAX_CANDIDATES = 20;

    private final YierdisDbKernel kernel;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final long expireCleanupTimeLimitNanos;

    YierdisDbExpirationSupport(
            YierdisDbKernel kernel,
            YierdisDbKeyLifecycle keyLifecycle,
            long expireCleanupTimeLimitNanos
    ) {
        this.kernel = Objects.requireNonNull(kernel, "kernel");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        if (expireCleanupTimeLimitNanos < 0L) {
            throw new IllegalArgumentException("expireCleanupTimeLimitNanos must be >= 0");
        }
        this.expireCleanupTimeLimitNanos = expireCleanupTimeLimitNanos;
    }

    int cleanupExpired() {
        return cleanupExpired(0L);
    }

    /**
     * 单次调用最多回收 {@value #CLEANUP_MAX_CANDIDATES} 个过期 key，并受时间预算限制；
     * 返回消费的索引项数（回收 + stale 丢弃），0 表示本次没有可推进的工作。
     */
    int cleanupExpired(long nowMillis) {
        kernel.checkOwner();
        long startedNanos = System.nanoTime();
        long nowFixed = nowMillis <= 0L ? System.currentTimeMillis() : nowMillis;
        int consumed = 0;
        int reclaimed = 0;
        while (true) {
            ExpiresIndex.Entry head = keyLifecycle.peekExpiresIndexEntry();
            if (head == null || head.expireAtMillis() > nowFixed) {
                return consumed;
            }
            EntryRecord live = liveRecord(head);
            if (live == null) {
                keyLifecycle.dropExpiresIndexHead();
                consumed++;
            } else if (kernel.reclaimExpired(head.keyHandle(), live, nowFixed)) {
                // 回收成功后才移除索引项；reclaim 抛异常时头部保留，下一次调用重试同一候选。
                keyLifecycle.dropExpiresIndexHead();
                consumed++;
                reclaimed++;
            } else {
                // 候选仍过期但本次未能删除：保留索引项，下一次调用重试同一候选。
                return consumed;
            }
            if (reclaimed >= CLEANUP_MAX_CANDIDATES || timeLimitReached(startedNanos)) {
                return consumed;
            }
        }
    }

    boolean hasDueExpiredCandidates(long nowMillis) {
        kernel.checkOwner();
        long nowFixed = nowMillis <= 0L ? System.currentTimeMillis() : nowMillis;
        return keyLifecycle.hasDueExpiresIndexEntry(nowFixed);
    }

    // 惰性校验三道关卡：key 仍在目录中、entry 仍持有同一 key identity、deadline 与索引项一致。
    // 任一不满足即判 stale；reclaimExpired 提交前还会再完整校验一次 record。
    private EntryRecord liveRecord(ExpiresIndex.Entry candidate) {
        final EntryRecord record;
        try {
            record = keyLifecycle.entryRecord(candidate.keyHandle());
        } catch (StaleNativeHandleException stale) {
            return null;
        }
        if (record == null
                || record.expireAtMillis() != candidate.expireAtMillis()
                || !record.keyHandle().equals(candidate.keyHandle().nativeHandle())) {
            return null;
        }
        return record;
    }

    private boolean timeLimitReached(long startedNanos) {
        return System.nanoTime() - startedNanos >= expireCleanupTimeLimitNanos;
    }
}
