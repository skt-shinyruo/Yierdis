package yier.bubu.redis.common.memory;

import java.util.Objects;

public record MemoryReclaimResult(
        long inspectedUnits,
        long reclaimedUnits,
        long reclaimedBytes,
        StopReason stopReason
) {
    private static final MemoryReclaimResult EMPTY = new MemoryReclaimResult(0, 0, 0, StopReason.COMPLETE);

    public MemoryReclaimResult {
        requireNonNegative(inspectedUnits, "inspectedUnits");
        requireNonNegative(reclaimedUnits, "reclaimedUnits");
        requireNonNegative(reclaimedBytes, "reclaimedBytes");
        Objects.requireNonNull(stopReason, "stopReason");
    }

    public static MemoryReclaimResult empty() {
        return EMPTY;
    }

    public enum StopReason {
        COMPLETE,
        INSPECTION_LIMIT,
        BYTE_LIMIT,
        TIME_LIMIT
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }
}
