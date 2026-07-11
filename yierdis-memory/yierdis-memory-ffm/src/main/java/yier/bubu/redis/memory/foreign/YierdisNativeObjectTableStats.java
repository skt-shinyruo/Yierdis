package yier.bubu.redis.memory.foreign;

import java.util.Arrays;

public final class YierdisNativeObjectTableStats {
    private final long metadataCommittedBytes;
    private final int activeSegments;
    private final long liveSlots;
    private final long freeSlots;
    private final long retiredSlots;
    private final long peakLiveSlots;
    private final long[] stateCounts;

    YierdisNativeObjectTableStats(
            long metadataCommittedBytes,
            int activeSegments,
            long liveSlots,
            long freeSlots,
            long retiredSlots,
            long peakLiveSlots,
            long[] stateCounts
    ) {
        this.metadataCommittedBytes = metadataCommittedBytes;
        this.activeSegments = activeSegments;
        this.liveSlots = liveSlots;
        this.freeSlots = freeSlots;
        this.retiredSlots = retiredSlots;
        this.peakLiveSlots = peakLiveSlots;
        this.stateCounts = Arrays.copyOf(stateCounts, stateCounts.length);
    }

    public long metadataCommittedBytes() {
        return metadataCommittedBytes;
    }

    public int activeSegments() {
        return activeSegments;
    }

    public long liveSlots() {
        return liveSlots;
    }

    public long freeSlots() {
        return freeSlots;
    }

    public long retiredSlots() {
        return retiredSlots;
    }

    public long peakLiveSlots() {
        return peakLiveSlots;
    }

    public long stateCount(int state) {
        if (state < 0 || state >= stateCounts.length) {
            throw new IllegalArgumentException("unknown native object state: " + state);
        }
        return stateCounts[state];
    }
}
