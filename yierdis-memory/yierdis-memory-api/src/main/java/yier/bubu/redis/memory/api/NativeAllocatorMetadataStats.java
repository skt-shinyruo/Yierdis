package yier.bubu.redis.memory.api;

public record NativeAllocatorMetadataStats(
        long activeMetadataSegments,
        long freeSlots
) {
    public NativeAllocatorMetadataStats {
        activeMetadataSegments = Math.max(0L, activeMetadataSegments);
        freeSlots = Math.max(0L, freeSlots);
    }
}
