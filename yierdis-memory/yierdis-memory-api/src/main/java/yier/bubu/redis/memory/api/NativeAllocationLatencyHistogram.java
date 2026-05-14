package yier.bubu.redis.memory.api;

public record NativeAllocationLatencyHistogram(
        long allocationCount,
        long totalNanos,
        long maxNanos,
        long under1Micros,
        long under10Micros,
        long under100Micros,
        long under1Millis,
        long atLeast1Millis
) {
    public NativeAllocationLatencyHistogram {
        requireNonNegative(allocationCount, "allocationCount");
        requireNonNegative(totalNanos, "totalNanos");
        requireNonNegative(maxNanos, "maxNanos");
        requireNonNegative(under1Micros, "under1Micros");
        requireNonNegative(under10Micros, "under10Micros");
        requireNonNegative(under100Micros, "under100Micros");
        requireNonNegative(under1Millis, "under1Millis");
        requireNonNegative(atLeast1Millis, "atLeast1Millis");
    }

    public static NativeAllocationLatencyHistogram empty() {
        return new NativeAllocationLatencyHistogram(0, 0, 0, 0, 0, 0, 0, 0);
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }
}
