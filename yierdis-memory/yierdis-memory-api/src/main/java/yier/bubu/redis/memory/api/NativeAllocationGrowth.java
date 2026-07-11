package yier.bubu.redis.memory.api;

import java.util.Objects;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;

public record NativeAllocationGrowth(
        long heapEstimatedBytes,
        long nativeMetadataCommittedBytes,
        long nativeDataCommittedBytes
) {
    private static final NativeAllocationGrowth ZERO = new NativeAllocationGrowth(0, 0, 0);

    public NativeAllocationGrowth {
        requireNonNegative(heapEstimatedBytes, "heapEstimatedBytes");
        requireNonNegative(nativeMetadataCommittedBytes, "nativeMetadataCommittedBytes");
        requireNonNegative(nativeDataCommittedBytes, "nativeDataCommittedBytes");
    }

    public long effectiveBytes() {
        return MemoryUsageSnapshot.addSaturating(
                MemoryUsageSnapshot.addSaturating(heapEstimatedBytes, nativeMetadataCommittedBytes),
                nativeDataCommittedBytes
        );
    }

    public NativeAllocationGrowth plus(NativeAllocationGrowth other) {
        Objects.requireNonNull(other, "other");
        return new NativeAllocationGrowth(
                MemoryUsageSnapshot.addSaturating(heapEstimatedBytes, other.heapEstimatedBytes),
                MemoryUsageSnapshot.addSaturating(
                        nativeMetadataCommittedBytes,
                        other.nativeMetadataCommittedBytes
                ),
                MemoryUsageSnapshot.addSaturating(nativeDataCommittedBytes, other.nativeDataCommittedBytes)
        );
    }

    public static NativeAllocationGrowth zero() {
        return ZERO;
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }
}
