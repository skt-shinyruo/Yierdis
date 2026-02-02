package yier.bubu.redis.db;

import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocator;

/**
 * Best-effort memory accounting utilities used for diagnostics.
 * <p>
 * The project intentionally avoids deep JVM-instrumentation; these numbers are explainable estimates.
 */
final class DbMemoryAccounting {
    private static final int ESTIMATED_HEAP_OBJECT_HEADER_BYTES = 16;

    private DbMemoryAccounting() {
    }

    static YierdisMemoryStats snapshot(
            long maxmemoryBytes,
            long heapDataBytesEstimate,
            YierdisOffHeapAllocator offHeapAllocator,
            YierdisKeyspace<?> store,
            YierdisExpireIndex expires,
            boolean keysStoredOffHeap,
            boolean includeOffHeapInMaxmemory
    ) {
        long offHeapUsedBytes = safeOffHeapUsedBytes(offHeapAllocator);

        int keyCount = store == null ? 0 : store.size();
        int expireCount = expires == null ? 0 : expires.size();

        boolean keyspaceRehashing = false;
        int keyspaceCap0 = 0;
        int keyspaceCap1 = 0;
        long keyspaceOverhead = 0;
        if (store instanceof ByteArrayKeyspace<?> ks) {
            keyspaceRehashing = ks.isRehashing();
            keyspaceCap0 = ks.table0Capacity();
            keyspaceCap1 = ks.table1Capacity();
            keyspaceOverhead = ks.estimatedTableOverheadBytes();
        }

        boolean expireRehashing = false;
        int expireCap0 = 0;
        int expireCap1 = 0;
        long expireOverhead = 0;
        long expireValueObjects = 0;
        if (expires instanceof YierdisHeapExpireIndex heap) {
            ByteArrayKeyspace<Long> raw = heap.rawKeyspace();
            expireRehashing = raw.isRehashing();
            expireCap0 = raw.table0Capacity();
            expireCap1 = raw.table1Capacity();
            expireOverhead = raw.estimatedTableOverheadBytes();
            expireValueObjects = estimateLongObjectBytes(expireCount);
        }

        long usedBytesForMaxmemory = heapDataBytesEstimate + (includeOffHeapInMaxmemory ? offHeapUsedBytes : 0);
        long totalEstimatedBytes =
                heapDataBytesEstimate
                        + offHeapUsedBytes
                        + keyspaceOverhead
                        + expireOverhead
                        + expireValueObjects;

        return new YierdisMemoryStats(
                maxmemoryBytes,
                usedBytesForMaxmemory,
                heapDataBytesEstimate,
                offHeapUsedBytes,
                keysStoredOffHeap,
                keyCount,
                expireCount,
                keyspaceRehashing,
                keyspaceCap0,
                keyspaceCap1,
                keyspaceOverhead,
                expireRehashing,
                expireCap0,
                expireCap1,
                expireOverhead,
                expireValueObjects,
                totalEstimatedBytes
        );
    }

    private static long safeOffHeapUsedBytes(YierdisOffHeapAllocator allocator) {
        if (allocator == null) {
            return 0;
        }
        try {
            return Math.max(0L, allocator.usedBytes());
        } catch (Throwable ignored) {
            return 0;
        }
    }

    private static long estimateLongObjectBytes(int count) {
        if (count <= 0) {
            return 0;
        }
        long one = align8((long) ESTIMATED_HEAP_OBJECT_HEADER_BYTES + Long.BYTES);
        return one * (long) count;
    }

    private static long align8(long bytes) {
        return (bytes + 7L) & ~7L;
    }
}
