package yier.bubu.redis.memory.api;

import java.util.Objects;

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
        NativeObjectKindCounts objectKindCounts,
        long metadataCommittedBytes,
        long activeMetadataSegments,
        long freeSlots,
        long retiredSlots,
        long peakLiveSlots
) {
    public NativeAllocatorStats {
        objectKindCounts = Objects.requireNonNull(objectKindCounts, "objectKindCounts");
    }

    public long objectCount(NativeObjectKind kind) {
        return objectKindCounts.count(kind);
    }
}
