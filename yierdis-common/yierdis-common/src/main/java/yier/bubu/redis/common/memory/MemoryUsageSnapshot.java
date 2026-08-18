package yier.bubu.redis.common.memory;

import java.util.Objects;

public record MemoryUsageSnapshot(
        long heapEstimatedBytes,
        long nativeMetadataCommittedBytes,
        long nativeDataCommittedBytes,
        long nativeDataLiveBytes,
        long nativeReclaimableBytes
) {
    public static final MemoryUsageSnapshot ZERO = new MemoryUsageSnapshot(0, 0, 0, 0, 0);

    public MemoryUsageSnapshot {
        requireNonNegative(heapEstimatedBytes, "heapEstimatedBytes");
        requireNonNegative(nativeMetadataCommittedBytes, "nativeMetadataCommittedBytes");
        requireNonNegative(nativeDataCommittedBytes, "nativeDataCommittedBytes");
        requireNonNegative(nativeDataLiveBytes, "nativeDataLiveBytes");
        requireNonNegative(nativeReclaimableBytes, "nativeReclaimableBytes");
    }

    public long effectiveBytesForMaxmemory() {
        return addSaturating(
                addSaturating(heapEstimatedBytes, nativeMetadataCommittedBytes),
                nativeDataCommittedBytes
        );
    }

    public MemoryUsageSnapshot plus(MemoryUsageSnapshot other) {
        Objects.requireNonNull(other, "other");
        return new MemoryUsageSnapshot(
                addSaturating(heapEstimatedBytes, other.heapEstimatedBytes),
                addSaturating(nativeMetadataCommittedBytes, other.nativeMetadataCommittedBytes),
                addSaturating(nativeDataCommittedBytes, other.nativeDataCommittedBytes),
                addSaturating(nativeDataLiveBytes, other.nativeDataLiveBytes),
                addSaturating(nativeReclaimableBytes, other.nativeReclaimableBytes)
        );
    }

    public static long addSaturating(long left, long right) {
        requireNonNegative(left, "left");
        requireNonNegative(right, "right");
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }
}
