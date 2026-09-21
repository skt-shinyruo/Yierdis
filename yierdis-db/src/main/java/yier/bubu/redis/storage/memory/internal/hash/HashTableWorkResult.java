package yier.bubu.redis.storage.memory.internal.hash;

import java.util.Objects;

public record HashTableWorkResult(
        long inspectedSlots,
        long migratedSlots,
        boolean rehashComplete,
        StopReason stopReason
) {
    public HashTableWorkResult {
        if (inspectedSlots < 0L) {
            throw new IllegalArgumentException("inspectedSlots must be >= 0");
        }
        if (migratedSlots < 0L || migratedSlots > inspectedSlots) {
            throw new IllegalArgumentException("migratedSlots must be in [0, inspectedSlots]");
        }
        Objects.requireNonNull(stopReason, "stopReason");
    }

    public enum StopReason {
        COMPLETE,
        SLOT_LIMIT,
        TIME_LIMIT,
        NOT_REHASHING
    }
}
