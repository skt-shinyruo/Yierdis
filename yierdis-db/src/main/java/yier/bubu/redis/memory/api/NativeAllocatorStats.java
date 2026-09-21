package yier.bubu.redis.memory.api;

public record NativeAllocatorStats(
        long logicalUsedBytes,
        long reservedBytes,
        long committedBytes,
        long freeBytes,
        long internalFragmentationBytes,
        long liveSmallPages,
        long liveMediumSpanPages,
        long liveLargeSpanPages,
        long liveObjects,
        long pinnedObjects,
        long quarantinedObjects,
        long staleHandleDetections,
        long reallocInPlaceCount,
        long reallocMovedCount,
        long defragMovedBytes,
        long defragSkippedPinnedObjects,
        long externalFragmentationBytes,
        long smallFreeBytes,
        long mediumFreeBytes,
        long largeFreeBytes,
        long freePages,
        long quarantineBytes,
        long doubleFreeDetections,
        long defragReclaimedPages,
        long metadataCommittedBytes,
        long activeMetadataSegments,
        long freeSlots,
        long retiredSlots,
        long peakLiveSlots
) {
}
