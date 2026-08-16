package yier.bubu.redis.storage.memory.internal.ledger;

import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.storage.api.YierdisMemoryStats;

/**
 * Best-effort memory accounting utilities used for diagnostics.
 * <p>
 * The project intentionally avoids deep JVM-instrumentation; these numbers are explainable estimates.
 */
public final class DbMemoryAccounting {
    private DbMemoryAccounting() {
    }

    public static YierdisMemoryStats snapshot(
            long maxmemoryBytes,
            MemoryUsageSnapshot usage,
            long reservedBytes,
            int keyCount,
            int expireCount,
            HashTableMaintenanceRegistry hashTableMaintenanceRegistry,
            boolean keysStoredOffHeap,
            NativeAllocatorStats nativeAllocatorStats,
            NativeDefragReport nativeDefragReport,
            long nativeLiveRegionCount
    ) {
        MemoryUsageSnapshot physicalUsage = usage == null ? MemoryUsageSnapshot.zero() : usage;
        long heapDataBytesEstimate = physicalUsage.heapEstimatedBytes();
        long offHeapUsedBytes = MemoryUsageSnapshot.addSaturating(
                physicalUsage.nativeMetadataCommittedBytes(),
                physicalUsage.nativeDataCommittedBytes()
        );

        boolean keyspaceRehashing = false;
        int keyspaceCap0 = 0;
        int keyspaceCap1 = 0;
        long keyspaceOverhead = 0;

        long totalEstimatedBytes = physicalUsage.effectiveBytesForMaxmemory();
        long usedBytesForMaxmemory = totalEstimatedBytes;
        long effectiveUsedBytesForMaxmemory = addSaturating(totalEstimatedBytes, Math.max(0L, reservedBytes));
        int pendingHashTableCount = hashTableMaintenanceRegistry == null
                ? 0
                : hashTableMaintenanceRegistry.pendingTableCount();
        String lastHashTableMaintenanceStopReason = hashTableMaintenanceRegistry == null
                ? "COMPLETE"
                : hashTableMaintenanceRegistry.lastStopReason().name();

        return new YierdisMemoryStats(
                maxmemoryBytes,
                usedBytesForMaxmemory,
                heapDataBytesEstimate,
                offHeapUsedBytes,
                reservedBytes,
                effectiveUsedBytesForMaxmemory,
                true,
                keysStoredOffHeap,
                keyCount,
                expireCount,
                keyspaceRehashing,
                keyspaceCap0,
                keyspaceCap1,
                keyspaceOverhead,
                false,
                0,
                0,
                0L,
                0L,
                totalEstimatedBytes,
                nativeDefragReport == null ? 0L : nativeDefragReport.scannedObjects(),
                nativeDefragReport == null ? 0L : nativeDefragReport.movedObjects(),
                nativeDefragReport == null ? 0L : nativeDefragReport.movedBytes(),
                nativeDefragReport == null ? 0L : nativeDefragReport.skippedPinnedObjects(),
                nativeDefragReport == null ? 0L : nativeDefragReport.skippedBudgetObjects(),
                nativeDefragReport == null ? 0L : nativeDefragReport.failedMoves(),
                nativeAllocatorStats == null ? 0L : nativeAllocatorStats.defragMovedBytes(),
                nativeAllocatorStats == null ? 0L : nativeAllocatorStats.defragSkippedPinnedObjects(),
                nativeAllocatorStats == null ? 0L : nativeAllocatorStats.quarantinedObjects(),
                nativeAllocatorStats == null ? 0L : nativeAllocatorStats.quarantineBytes(),
                nativeAllocatorStats == null ? 0L : nativeAllocatorStats.staleHandleDetections(),
                nativeAllocatorStats == null ? 0L : nativeAllocatorStats.defragReclaimedPages(),
                physicalUsage.nativeMetadataCommittedBytes(),
                physicalUsage.nativeDataCommittedBytes(),
                physicalUsage.nativeDataLiveBytes(),
                physicalUsage.nativeReclaimableBytes(),
                pendingHashTableCount,
                lastHashTableMaintenanceStopReason,
                nativeAllocatorStats == null ? 0L : nativeAllocatorStats.liveObjects(),
                Math.max(0L, nativeLiveRegionCount)
        );
    }

    private static long addSaturating(long left, long right) {
        if (left < 0 || right < 0) {
            return Long.MAX_VALUE;
        }
        if (Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }
}
