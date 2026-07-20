package yier.bubu.redis.runtime.embedded;

import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.storage.api.DbHealthSnapshot;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
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

    public CommitStreamStats commitStreamStats() {
        CommitStream stream = instance.commitStream();
        return stream == null ? CommitStreamStats.disabled() : stream.stats();
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

        MemoryUsageSnapshot physicalUsage = MemoryUsageSnapshot.zero();
        long keyspaceOverhead = 0;
        long expireOverhead = 0;
        long expireValueObjects = 0;
        long reserved = 0;
        int keyCount = 0;
        int expireCount = 0;
        boolean keysStoredOffHeap = false;
        boolean keyspaceRehashing = false;
        boolean expireRehashing = false;
        int keyspaceCap0 = 0;
        int keyspaceCap1 = 0;
        int expireCap0 = 0;
        int expireCap1 = 0;
        long nativeDefragLastScannedObjects = 0;
        long nativeDefragLastMovedObjects = 0;
        long nativeDefragLastMovedBytes = 0;
        long nativeDefragLastSkippedPinnedObjects = 0;
        long nativeDefragLastSkippedBudgetObjects = 0;
        long nativeDefragLastFailedMoves = 0;
        long nativeDefragMovedBytes = 0;
        long nativeDefragSkippedPinnedObjects = 0;
        long nativeDefragQuarantinedObjects = 0;
        long nativeDefragQuarantineBytes = 0;
        long nativeStaleHandleDetections = 0;
        long nativeDefragReclaimedPages = 0;
        long nativeLiveObjects = 0;
        long nativeLiveRegions = 0;
        long expiredEntriesAwaitingPhysicalDeletion = 0;
        int pendingHashTableCount = 0;
        String lastHashTableMaintenanceStopReason = "COMPLETE";
        boolean sharedNativeRuntime = instance.config().maxmemoryScope() == YierdisInstanceConfig.MaxmemoryScope.GLOBAL;

        for (int dbIndex = 0; dbIndex < databases; dbIndex++) {
            var engine = instance.runtimeEngine(dbIndex);
            MemoryUsageSnapshot dbUsage = engine.memoryUsage();
            if (dbUsage != null) {
                physicalUsage = physicalUsage.plus(dbUsage);
            }
            YierdisMemoryStats s = engine.memory().memoryStats();
            keyspaceOverhead = addSaturating(keyspaceOverhead, s.keyspaceTableOverheadBytesEstimate());
            expireOverhead = addSaturating(expireOverhead, s.expireTableOverheadBytesEstimate());
            expireValueObjects = addSaturating(expireValueObjects, s.expireValueObjectsBytesEstimate());
            reserved = addSaturating(reserved, Math.max(0L, s.reservedBytes()));
            keyCount = addSaturating(keyCount, s.keyCount());
            expireCount = addSaturating(expireCount, s.expireCount());
            keysStoredOffHeap |= s.keysStoredOffHeap();
            keyspaceRehashing |= s.keyspaceRehashing();
            expireRehashing |= s.expireRehashing();
            keyspaceCap0 = addSaturating(keyspaceCap0, s.keyspaceTable0Capacity());
            keyspaceCap1 = addSaturating(keyspaceCap1, s.keyspaceTable1Capacity());
            expireCap0 = addSaturating(expireCap0, s.expireTable0Capacity());
            expireCap1 = addSaturating(expireCap1, s.expireTable1Capacity());
            nativeDefragLastScannedObjects = addSaturating(nativeDefragLastScannedObjects, s.nativeDefragLastScannedObjects());
            nativeDefragLastMovedObjects = addSaturating(nativeDefragLastMovedObjects, s.nativeDefragLastMovedObjects());
            nativeDefragLastMovedBytes = addSaturating(nativeDefragLastMovedBytes, s.nativeDefragLastMovedBytes());
            nativeDefragLastSkippedPinnedObjects = addSaturating(nativeDefragLastSkippedPinnedObjects, s.nativeDefragLastSkippedPinnedObjects());
            nativeDefragLastSkippedBudgetObjects = addSaturating(nativeDefragLastSkippedBudgetObjects, s.nativeDefragLastSkippedBudgetObjects());
            nativeDefragLastFailedMoves = addSaturating(nativeDefragLastFailedMoves, s.nativeDefragLastFailedMoves());
            nativeDefragMovedBytes = addSaturating(nativeDefragMovedBytes, s.nativeDefragMovedBytes());
            nativeDefragSkippedPinnedObjects = addSaturating(nativeDefragSkippedPinnedObjects, s.nativeDefragSkippedPinnedObjects());
            nativeDefragQuarantinedObjects = addSaturating(nativeDefragQuarantinedObjects, s.nativeDefragQuarantinedObjects());
            nativeDefragQuarantineBytes = addSaturating(nativeDefragQuarantineBytes, s.nativeDefragQuarantineBytes());
            nativeStaleHandleDetections = addSaturating(nativeStaleHandleDetections, s.nativeStaleHandleDetections());
            nativeDefragReclaimedPages = addSaturating(nativeDefragReclaimedPages, s.nativeDefragReclaimedPages());
            nativeLiveObjects = addSaturating(nativeLiveObjects, s.nativeLiveObjects());
            expiredEntriesAwaitingPhysicalDeletion = addSaturating(
                    expiredEntriesAwaitingPhysicalDeletion,
                    s.expiredEntriesAwaitingPhysicalDeletion()
            );
            if (sharedNativeRuntime) {
                nativeLiveRegions = Math.max(nativeLiveRegions, Math.max(0L, s.nativeLiveRegions()));
            } else {
                nativeLiveRegions = addSaturating(nativeLiveRegions, s.nativeLiveRegions());
            }
            pendingHashTableCount = addSaturating(pendingHashTableCount, s.pendingHashTableCount());
            if (!"COMPLETE".equals(s.lastHashTableMaintenanceStopReason())) {
                lastHashTableMaintenanceStopReason = s.lastHashTableMaintenanceStopReason();
            }
        }

        long offHeap = MemoryUsageSnapshot.addSaturating(
                physicalUsage.nativeMetadataCommittedBytes(),
                physicalUsage.nativeDataCommittedBytes()
        );
        long totalEstimatedBytes = physicalUsage.effectiveBytesForMaxmemory();
        long effectiveUsedBytesForMaxmemory = MemoryUsageSnapshot.addSaturating(totalEstimatedBytes, reserved);

        return new YierdisMemoryStats(
                instance.config().maxmemoryBytes(),
                totalEstimatedBytes,
                physicalUsage.heapEstimatedBytes(),
                offHeap,
                reserved,
                effectiveUsedBytesForMaxmemory,
                true,
                keysStoredOffHeap,
                keyCount,
                expireCount,
                keyspaceRehashing,
                keyspaceCap0,
                keyspaceCap1,
                keyspaceOverhead,
                expireRehashing,
                expireCap0,
                expireCap1,
                expireOverhead,
                expireValueObjects,
                totalEstimatedBytes,
                nativeDefragLastScannedObjects,
                nativeDefragLastMovedObjects,
                nativeDefragLastMovedBytes,
                nativeDefragLastSkippedPinnedObjects,
                nativeDefragLastSkippedBudgetObjects,
                nativeDefragLastFailedMoves,
                nativeDefragMovedBytes,
                nativeDefragSkippedPinnedObjects,
                nativeDefragQuarantinedObjects,
                nativeDefragQuarantineBytes,
                nativeStaleHandleDetections,
                nativeDefragReclaimedPages,
                physicalUsage.nativeMetadataCommittedBytes(),
                physicalUsage.nativeDataCommittedBytes(),
                physicalUsage.nativeDataLiveBytes(),
                physicalUsage.nativeReclaimableBytes(),
                pendingHashTableCount,
                lastHashTableMaintenanceStopReason,
                nativeLiveObjects,
                nativeLiveRegions,
                expiredEntriesAwaitingPhysicalDeletion
        );
    }

    public List<YierdisDbSummary> dbSummaries() {
        int databases = Math.max(0, instance.databases());
        if (databases == 0) {
            return List.of();
        }
        List<YierdisDbSummary> summaries = new ArrayList<>(databases);
        for (int dbIndex = 0; dbIndex < databases; dbIndex++) {
            YierdisMemoryStats s = instance.runtimeEngine(dbIndex).memory().memoryStats();
            summaries.add(new YierdisDbSummary(dbIndex, s.keyCount(), s.expireCount()));
        }
        return summaries;
    }

    private static YierdisMemoryStats emptyStats(long maxmemoryBytes, boolean offHeapIncludedInMaxmemory) {
        return new YierdisMemoryStats(
                maxmemoryBytes,
                0,
                0,
                0,
                0,
                0,
                offHeapIncludedInMaxmemory,
                false,
                0,
                0,
                false,
                0,
                0,
                0,
                false,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
        );
    }

    private static long addSaturating(long left, long right) {
        if (right <= 0) {
            return left;
        }
        if (left >= Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static int addSaturating(int left, int right) {
        if (right <= 0) {
            return left;
        }
        if (left >= Integer.MAX_VALUE - right) {
            return Integer.MAX_VALUE;
        }
        return left + right;
    }
}
