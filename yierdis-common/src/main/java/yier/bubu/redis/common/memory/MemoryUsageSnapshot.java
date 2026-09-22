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

    /**
     * 全仓库 main 源唯一的饱和加法。调用点都处于字节数/计数器等非负域：任一操作数为负或求和溢出时
     * 一律收敛到 {@link Long#MAX_VALUE}，绝不回绕成较小值——负数视为上游账目漂移，按已饱和保守处理。
     */
    public static long addSaturating(long left, long right) {
        if (left < 0L || right < 0L || left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }
}
