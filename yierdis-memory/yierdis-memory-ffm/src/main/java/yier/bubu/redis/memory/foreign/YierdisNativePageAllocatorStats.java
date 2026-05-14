package yier.bubu.redis.memory.foreign;

public record YierdisNativePageAllocatorStats(
        long committedBytes,
        long usedBytes,
        long freeBytes,
        long liveSmallPages,
        long liveMediumSpanPages,
        long liveLargeSpanPages,
        long smallFreeBytes,
        long mediumFreeBytes,
        long largeFreeBytes,
        long freePages
) {
    public YierdisNativePageAllocatorStats(
            long committedBytes,
            long usedBytes,
            long freeBytes,
            long liveSmallPages,
            long liveMediumSpanPages,
            long liveLargeSpanPages
    ) {
        this(
                committedBytes,
                usedBytes,
                freeBytes,
                liveSmallPages,
                liveMediumSpanPages,
                liveLargeSpanPages,
                freeBytes,
                0,
                0,
                0
        );
    }
}
