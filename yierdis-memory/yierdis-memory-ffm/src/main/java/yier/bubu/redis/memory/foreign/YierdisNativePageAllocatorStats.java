package yier.bubu.redis.memory.foreign;

public record YierdisNativePageAllocatorStats(
        long committedBytes,
        long usedBytes,
        long freeBytes,
        long liveSmallPages,
        long liveMediumSpanPages,
        long liveLargeSpanPages
) {
}
