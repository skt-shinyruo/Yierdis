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
        NativeAllocationLatencyHistogram allocationLatencyHistogram
) {
    public NativeAllocatorStats(
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
            long defragSkippedPinnedObjects
    ) {
        this(
                logicalUsedBytes,
                reservedBytes,
                committedBytes,
                freeBytes,
                internalFragmentationBytes,
                liveSmallPages,
                liveMediumSpanPages,
                liveLargeSpanPages,
                liveObjects,
                pinnedObjects,
                quarantinedObjects,
                staleHandleDetections,
                reallocInPlaceCount,
                reallocMovedCount,
                defragMovedBytes,
                defragSkippedPinnedObjects,
                0,
                freeBytes,
                0,
                0,
                0,
                0,
                0,
                0,
                NativeObjectKindCounts.empty(),
                NativeAllocationLatencyHistogram.empty()
        );
    }

    public NativeAllocatorStats {
        objectKindCounts = Objects.requireNonNull(objectKindCounts, "objectKindCounts");
        allocationLatencyHistogram = Objects.requireNonNull(
                allocationLatencyHistogram,
                "allocationLatencyHistogram"
        );
    }

    public long objectCount(NativeObjectKind kind) {
        return objectKindCounts.count(kind);
    }
}
