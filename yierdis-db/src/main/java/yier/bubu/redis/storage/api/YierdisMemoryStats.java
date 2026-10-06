package yier.bubu.redis.storage.api;

import java.util.Objects;

/**
 * maxmemory / eviction 的内存预算分解，供 {@code MEMORY STATS} 与 {@code INFO} 读取。
 * <p>
 * {@link #ledgerUsedBytes()} 是准入账本的逻辑用量，不是堆估算。
 * 其余字节字段保持原有的估算或提交量口径，不是精确的 JVM 堆测量。
 */
public record YierdisMemoryStats(
        long maxmemoryBytes,
        long usedBytesForMaxmemory,
        long heapDataBytesEstimate,
        long offHeapUsedBytes,
        long reservedBytes,
        long effectiveUsedBytesForMaxmemory,
        boolean offHeapIncludedInMaxmemory,
        boolean keysStoredOffHeap,
        int keyCount,
        int expireCount,
        long totalEstimatedBytes,
        long nativeMetadataCommittedBytes,
        long nativeDataCommittedBytes,
        long nativeDataLiveBytes,
        int pendingHashTableCount,
        String lastHashTableMaintenanceStopReason,
        long nativeLiveObjects,
        long nativeLiveRegions,
        /**
         * 准入账本的逻辑用量（{@code MemoryLedger.usedBytes()}），不是堆估算。
         */
        long ledgerUsedBytes
) {
    public YierdisMemoryStats {
        if (pendingHashTableCount < 0) {
            throw new IllegalArgumentException("pendingHashTableCount must be >= 0");
        }
        Objects.requireNonNull(lastHashTableMaintenanceStopReason, "lastHashTableMaintenanceStopReason");
    }

    public static YierdisMemoryStats empty(long maxmemoryBytes, boolean offHeapIncludedInMaxmemory) {
        return new YierdisMemoryStats(
                maxmemoryBytes,
                0L,
                0L,
                0L,
                0L,
                0L,
                offHeapIncludedInMaxmemory,
                false,
                0,
                0,
                0L,
                0L,
                0L,
                0L,
                0,
                "COMPLETE",
                0L,
                0L,
                0L
        );
    }
}
