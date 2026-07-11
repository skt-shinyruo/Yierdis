package yier.bubu.redis.common.memory;

public record MemoryPressureBudget(
        long maxInspectedUnits,
        long maxReclaimedBytes,
        long timeLimitNanos
) {
    private static final MemoryPressureBudget UNLIMITED = new MemoryPressureBudget(
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            Long.MAX_VALUE
    );

    public MemoryPressureBudget {
        requireNonNegative(maxInspectedUnits, "maxInspectedUnits");
        requireNonNegative(maxReclaimedBytes, "maxReclaimedBytes");
        requireNonNegative(timeLimitNanos, "timeLimitNanos");
    }

    public static MemoryPressureBudget unlimited() {
        return UNLIMITED;
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }
}
