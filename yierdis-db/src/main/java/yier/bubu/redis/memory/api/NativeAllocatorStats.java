package yier.bubu.redis.memory.api;

public record NativeAllocatorStats(
        long logicalUsedBytes,
        long reservedBytes,
        long committedBytes,
        long freeBytes,
        long internalFragmentationBytes,
        long liveSmallPages,
        long liveMediumPages,
        long liveLargePages,
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
        long emptySmallPages,
        long quarantineBytes,
        long staleHandleFreeDetections,
        long defragRetiredBlockPages,
        long defragTrimReclaimedPages,
        long metadataCommittedBytes,
        long activeMetadataSegments,
        long freeSlots,
        long retiredSlots,
        long peakLiveSlots
) {
}
