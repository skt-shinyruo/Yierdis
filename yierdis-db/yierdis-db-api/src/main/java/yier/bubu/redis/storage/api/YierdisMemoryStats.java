package yier.bubu.redis.storage.api;

import java.util.Objects;

/**
 * A best-effort memory budget breakdown for explaining maxmemory / eviction behavior.
 * <p>
 * This is an <b>estimate</b> designed to be stable and explainable, not a precise JVM heap measurement.
 */
public record YierdisMemoryStats(
        long maxmemoryBytes,
        long usedBytesForMaxmemory,
        long heapDataBytesEstimate,
        long offHeapUsedBytes,
        long reservedBytes,
        long effectiveUsedBytesForMaxmemory,
        boolean offHeapIncludedInMaxmemory,
        boolean keysStoredOffHeap,
        int keyCount,
        int expireCount,
        boolean keyspaceRehashing,
        int keyspaceTable0Capacity,
        int keyspaceTable1Capacity,
        long keyspaceTableOverheadBytesEstimate,
        boolean expireRehashing,
        int expireTable0Capacity,
        int expireTable1Capacity,
        long expireTableOverheadBytesEstimate,
        long expireValueObjectsBytesEstimate,
        long totalEstimatedBytes,
        long nativeDefragLastScannedObjects,
        long nativeDefragLastMovedObjects,
        long nativeDefragLastMovedBytes,
        long nativeDefragLastSkippedPinnedObjects,
        long nativeDefragLastSkippedBudgetObjects,
        long nativeDefragLastFailedMoves,
        long nativeDefragMovedBytes,
        long nativeDefragSkippedPinnedObjects,
        long nativeDefragQuarantinedObjects,
        long nativeDefragQuarantineBytes,
        long nativeStaleHandleDetections,
        long nativeDefragReclaimedPages,
        long nativeMetadataCommittedBytes,
        long nativeDataCommittedBytes,
        long nativeDataLiveBytes,
        long nativeReclaimableBytes,
        int pendingHashTableCount,
        String lastHashTableMaintenanceStopReason,
        long nativeLiveObjects,
        long nativeLiveRegions
) {
    public YierdisMemoryStats {
        if (pendingHashTableCount < 0) {
            throw new IllegalArgumentException("pendingHashTableCount must be >= 0");
        }
        Objects.requireNonNull(lastHashTableMaintenanceStopReason, "lastHashTableMaintenanceStopReason");
    }

    public static YierdisMemoryStats empty(long maxmemoryBytes, boolean offHeapIncludedInMaxmemory) {
        return new YierdisMemoryStats(
                maxmemoryBytes,
                0L,
                0L,
                0L,
                0L,
                0L,
                offHeapIncludedInMaxmemory,
                false,
                0,
                0,
                false,
                0,
                0,
                0L,
                false,
                0,
                0,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0,
                "COMPLETE",
                0L,
                0L
        );
    }
}
