package yier.bubu.redis.runtime.embedded;

import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.storage.api.DbHealthSnapshot;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import static yier.bubu.redis.common.memory.MemoryUsageSnapshot.addSaturating;

import yier.bubu.redis.common.memory.MemoryUsageSnapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Runtime-owned observability seam for instance-wide aggregation.
 */
public final class YierdisInstanceObservability {
    private final YierdisInstance instance;

    public record YierdisDbSummary(int dbIndex, int keyCount, int expireCount) {
    }

    /** 供 server readiness 视图使用的 DB 健康聚合结果。 */
    public record RuntimeHealthSnapshot(
            int databaseCount,
            int degradedDatabaseCount,
            String firstFailureType,
            String firstFailureMessage,
            long firstFailureAtMillis
    ) {
        public boolean healthy() {
            return degradedDatabaseCount == 0;
        }
    }

    YierdisInstanceObservability(YierdisInstance instance) {
        this.instance = Objects.requireNonNull(instance, "instance");
    }

    /**
     * 在 DB owner thread 上聚合当前健康状态。
     * <p>
     * observability 层不拥有调度器；server 或 embedded 调用方若位于其他线程，必须先调度到 instance 的 owner thread。
     */
    public RuntimeHealthSnapshot healthSnapshot() {
        int databases = Math.max(0, instance.databases());
        int degraded = 0;
        String firstType = null;
        String firstMessage = null;
        long firstAt = 0L;
        for (int dbIndex = 0; dbIndex < databases; dbIndex++) {
            DbHealthSnapshot health = instance.runtimeEngine(dbIndex).health();
            if (health == null || !health.degraded()) {
                continue;
            }
            degraded++;
            long failureAt = health.failureAtMillis();
            if (firstAt == 0L || (failureAt > 0L && failureAt < firstAt)) {
                firstType = health.failureTypeName();
                firstMessage = health.failureMessage();
                firstAt = failureAt;
            }
        }
        return new RuntimeHealthSnapshot(databases, degraded, firstType, firstMessage, firstAt);
    }

    public YierdisMemoryStats memoryStats() {
        int databases = Math.max(0, instance.databases());
        if (databases == 0) {
            // With no DBs, off-heap is effectively 0; treat it as included to avoid surprising UI/metrics.
            return emptyStats(instance.config().maxmemoryBytes(), true);
        }

        MemoryUsageSnapshot physicalUsage = MemoryUsageSnapshot.ZERO;
        long reserved = 0;
        long keyCount = 0L;
        long expireCount = 0L;
        boolean keysStoredOffHeap = false;
        long nativeLiveObjects = 0;
        long nativeLiveRegions = 0;
        long pendingHashTableCount = 0L;
        String lastHashTableMaintenanceStopReason = "COMPLETE";
        boolean sharedNativeRuntime = instance.config().maxmemoryScope() == YierdisInstanceConfig.MaxmemoryScope.GLOBAL;

        for (int dbIndex = 0; dbIndex < databases; dbIndex++) {
            var engine = instance.runtimeEngine(dbIndex);
            YierdisMemoryStats s = engine.memoryStats();
            // global capability 的 snapshot 是 admission 权威物理视图；baseline 则从同一次语义 stats 读取投影五个物理字段。
            MemoryUsageSnapshot dbUsage = engine instanceof MaxmemoryParticipant participant
                    ? participant.memoryUsage()
                    : new MemoryUsageSnapshot(
                            s.heapDataBytesEstimate(),
                            s.nativeMetadataCommittedBytes(),
                            s.nativeDataCommittedBytes(),
                            s.nativeDataLiveBytes(),
                            0L
                    );
            if (dbUsage != null) {
                physicalUsage = physicalUsage.plus(dbUsage);
            }
            reserved = addSaturating(reserved, Math.max(0L, s.reservedBytes()));
            keyCount = addSaturating(keyCount, Math.max(0L, s.keyCount()));
            expireCount = addSaturating(expireCount, Math.max(0L, s.expireCount()));
            keysStoredOffHeap |= s.keysStoredOffHeap();
            nativeLiveObjects = addSaturating(nativeLiveObjects, Math.max(0L, s.nativeLiveObjects()));
            if (sharedNativeRuntime) {
                nativeLiveRegions = Math.max(nativeLiveRegions, Math.max(0L, s.nativeLiveRegions()));
            } else {
                nativeLiveRegions = addSaturating(nativeLiveRegions, Math.max(0L, s.nativeLiveRegions()));
            }
            pendingHashTableCount = addSaturating(pendingHashTableCount, Math.max(0L, s.pendingHashTableCount()));
            if (!"COMPLETE".equals(s.lastHashTableMaintenanceStopReason())) {
                lastHashTableMaintenanceStopReason = s.lastHashTableMaintenanceStopReason();
            }
        }

        long offHeap = addSaturating(
                physicalUsage.nativeMetadataCommittedBytes(),
                physicalUsage.nativeDataCommittedBytes()
        );
        long totalEstimatedBytes = physicalUsage.effectiveBytesForMaxmemory();
        long effectiveUsedBytesForMaxmemory = addSaturating(totalEstimatedBytes, reserved);

        return new YierdisMemoryStats(
                instance.config().maxmemoryBytes(),
                totalEstimatedBytes,
                physicalUsage.heapEstimatedBytes(),
                offHeap,
                reserved,
                effectiveUsedBytesForMaxmemory,
                true,
                keysStoredOffHeap,
                (int) Math.min(Integer.MAX_VALUE, keyCount),
                (int) Math.min(Integer.MAX_VALUE, expireCount),
                totalEstimatedBytes,
                physicalUsage.nativeMetadataCommittedBytes(),
                physicalUsage.nativeDataCommittedBytes(),
                physicalUsage.nativeDataLiveBytes(),
                (int) Math.min(Integer.MAX_VALUE, pendingHashTableCount),
                lastHashTableMaintenanceStopReason,
                nativeLiveObjects,
                nativeLiveRegions
        );
    }

    public List<YierdisDbSummary> dbSummaries() {
        int databases = Math.max(0, instance.databases());
        if (databases == 0) {
            return List.of();
        }
        List<YierdisDbSummary> summaries = new ArrayList<>(databases);
        for (int dbIndex = 0; dbIndex < databases; dbIndex++) {
            YierdisMemoryStats s = instance.runtimeEngine(dbIndex).memoryStats();
            summaries.add(new YierdisDbSummary(dbIndex, s.keyCount(), s.expireCount()));
        }
        return summaries;
    }

    private static YierdisMemoryStats emptyStats(long maxmemoryBytes, boolean offHeapIncludedInMaxmemory) {
        return YierdisMemoryStats.empty(maxmemoryBytes, offHeapIncludedInMaxmemory);
    }
}
