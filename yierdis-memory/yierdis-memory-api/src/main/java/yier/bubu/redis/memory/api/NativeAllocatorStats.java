package yier.bubu.redis.memory.api;

public record NativeAllocatorStats(
        long logicalUsedBytes,
        long reservedBytes,
        long liveObjects,
        long pinnedObjects,
        long quarantinedObjects,
        long staleHandleDetections,
        long reallocInPlaceCount,
        long reallocMovedCount
) {
}
