package yier.bubu.redis.storage.memory.internal.ledger;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmExpireIndex;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.YierdisMemoryStats;

/**
 * Best-effort memory accounting utilities used for diagnostics.
 * <p>
 * The project intentionally avoids deep JVM-instrumentation; these numbers are explainable estimates.
 */
public final class DbMemoryAccounting {
    private static final int ESTIMATED_HEAP_OBJECT_HEADER_BYTES = 16;

    private DbMemoryAccounting() {
    }

    public static YierdisMemoryStats snapshot(
            long maxmemoryBytes,
            long heapDataBytesEstimate,
            long reservedBytes,
            long directNativeBytes,
            int keyCount,
            YierdisExpireIndex expires,
            boolean keysStoredOffHeap,
            boolean includeOffHeapInMaxmemory,
            NativeAllocatorStats nativeAllocatorStats,
            NativeDefragReport nativeDefragReport
    ) {
        long directNativeUsedBytes = Math.max(0L, directNativeBytes);
        long offHeapUsedBytes = directNativeUsedBytes;

        int expireCount = expires == null ? 0 : expires.size();

        boolean keyspaceRehashing = false;
        int keyspaceCap0 = 0;
        int keyspaceCap1 = 0;
        long keyspaceOverhead = 0;

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
        } else if (expires instanceof YierdisFfmExpireIndex ffm) {
            expireRehashing = ffm.isRehashing();
            expireCap0 = ffm.table0Capacity();
            expireCap1 = ffm.table1Capacity();
            expireOverhead = ffm.estimatedTableOverheadBytes();
        }

        long usedBytesForMaxmemory = heapDataBytesEstimate + (includeOffHeapInMaxmemory ? offHeapUsedBytes : 0);
        long ttlBytesEstimate = estimateTtlBytesForMaxmemory(expireCount);
        if (ttlBytesEstimate > 0) {
            if (Long.MAX_VALUE - usedBytesForMaxmemory < ttlBytesEstimate) {
                usedBytesForMaxmemory = Long.MAX_VALUE;
            } else {
                usedBytesForMaxmemory += ttlBytesEstimate;
            }
        }
        long effectiveUsedBytesForMaxmemory = usedBytesForMaxmemory + Math.max(0L, reservedBytes);
        long totalEstimatedBytes = heapDataBytesEstimate + directNativeUsedBytes;
        if (expires instanceof YierdisHeapExpireIndex) {
            totalEstimatedBytes += expireOverhead + expireValueObjects;
        }

        return new YierdisMemoryStats(
                maxmemoryBytes,
                usedBytesForMaxmemory,
                heapDataBytesEstimate,
                offHeapUsedBytes,
                reservedBytes,
                effectiveUsedBytesForMaxmemory,
                includeOffHeapInMaxmemory,
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
                totalEstimatedBytes,
                nativeDefragReport == null ? 0L : nativeDefragReport.scannedObjects(),
                nativeDefragReport == null ? 0L : nativeDefragReport.movedObjects(),
                nativeDefragReport == null ? 0L : nativeDefragReport.movedBytes(),
                nativeDefragReport == null ? 0L : nativeDefragReport.skippedPinnedObjects(),
                nativeDefragReport == null ? 0L : nativeDefragReport.skippedBudgetObjects(),
                nativeDefragReport == null ? 0L : nativeDefragReport.failedMoves(),
                nativeAllocatorStats == null ? 0L : nativeAllocatorStats.defragMovedBytes(),
                nativeAllocatorStats == null ? 0L : nativeAllocatorStats.defragSkippedPinnedObjects(),
                nativeAllocatorStats == null ? 0L : nativeAllocatorStats.quarantinedObjects(),
                nativeAllocatorStats == null ? 0L : nativeAllocatorStats.quarantineBytes(),
                nativeAllocatorStats == null ? 0L : nativeAllocatorStats.staleHandleDetections(),
                nativeAllocatorStats == null ? 0L : nativeAllocatorStats.defragReclaimedPages()
        );
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

    private static long estimateTtlBytesForMaxmemory(int expireCount) {
        long perEntry = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
        if (perEntry <= 0 || expireCount <= 0) {
            return 0;
        }
        try {
            return Math.multiplyExact((long) expireCount, perEntry);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }
}
