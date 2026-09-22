package yier.bubu.redis.memory.foreign;

record YierdisNativeObjectTableStats(
        long metadataCommittedBytes,
        int activeSegments,
        long liveSlots,
        long freeSlots,
        long retiredSlots,
        long peakLiveSlots
) {
}
