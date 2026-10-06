package yier.bubu.redis.memory.foreign;

record YierdisNativePageAllocatorStats(
        long committedBytes,
        long usedBytes,
        long freeBytes,
        long liveSmallPages,
        long liveMediumPages,
        long liveLargePages,
        long smallFreeBytes,
        long emptySmallPages,
        long livePageRegistryEntries,
        long liveSpanDescriptors,
        long pageRegistryHeapBytes
) {
}
