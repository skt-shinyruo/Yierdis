package yier.bubu.redis.storage.memory.internal.hash;

import java.util.Objects;

public record HashTableMaintenanceResult(
        long inspectedSlots,
        long migratedSlots,
        int pendingTableCount,
        StopReason stopReason
) {
    public HashTableMaintenanceResult {
        if (inspectedSlots < 0L) {
            throw new IllegalArgumentException("inspectedSlots must be >= 0");
        }
        if (migratedSlots < 0L || migratedSlots > inspectedSlots) {
            throw new IllegalArgumentException("migratedSlots must be in [0, inspectedSlots]");
        }
        if (pendingTableCount < 0) {
            throw new IllegalArgumentException("pendingTableCount must be >= 0");
        }
        Objects.requireNonNull(stopReason, "stopReason");
    }

    public enum StopReason {
        COMPLETE,
        SLOT_LIMIT,
        TIME_LIMIT,
        CAPACITY_LIMIT,
        NO_PROGRESS
    }
}
