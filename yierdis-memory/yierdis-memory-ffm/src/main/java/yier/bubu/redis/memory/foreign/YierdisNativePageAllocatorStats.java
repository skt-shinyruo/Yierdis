package yier.bubu.redis.memory.foreign;

record YierdisNativePageAllocatorStats(
        long committedBytes,
        long usedBytes,
        long freeBytes,
        long liveSmallPages,
        long liveMediumSpanPages,
        long liveLargeSpanPages,
        long smallFreeBytes,
        long mediumFreeBytes,
        long largeFreeBytes,
        long freePages,
        long emptySmallPages,
        long livePageRegistryEntries,
        long liveSpanDescriptors,
        long pageRegistryHeapBytes
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
                0,
                0,
                0,
                0,
                0
        );
    }
}
