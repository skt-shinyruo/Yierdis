package yier.bubu.redis.db;

/**
 * A best-effort memory budget breakdown for explaining maxmemory / eviction behavior.
 * <p>
 * This is an <b>estimate</b> designed to be stable and explainable, not a precise JVM heap measurement.
 */
public record YierdisMemoryStats(
        long maxmemoryBytes,
        long usedBytesForMaxmemory,
        long heapDataBytesEstimate,
        long offHeapUsedBytes,
        boolean keysStoredOffHeap,
        int keyCount,
        int expireCount,
        boolean keyspaceRehashing,
        int keyspaceTable0Capacity,
        int keyspaceTable1Capacity,
        long keyspaceTableOverheadBytesEstimate,
        boolean expireRehashing,
        int expireTable0Capacity,
        int expireTable1Capacity,
        long expireTableOverheadBytesEstimate,
        long expireValueObjectsBytesEstimate,
        long totalEstimatedBytes
) {
}

