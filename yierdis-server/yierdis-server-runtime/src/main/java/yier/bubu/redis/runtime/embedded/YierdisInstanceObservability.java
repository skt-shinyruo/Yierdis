package yier.bubu.redis.runtime.embedded;

import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Runtime-owned observability seam for instance-wide aggregation.
 */
public final class YierdisInstanceObservability {
    private final YierdisInstance instance;

    public record YierdisDbSummary(int dbIndex, int keyCount, int expireCount) {
    }

    YierdisInstanceObservability(YierdisInstance instance) {
        this.instance = Objects.requireNonNull(instance, "instance");
    }

    public YierdisMemoryStats memoryStats() {
        boolean globalScope = instance.config().maxmemoryScope() == YierdisInstanceConfig.MaxmemoryScope.GLOBAL;
        int databases = Math.max(0, instance.databases());
        if (databases == 0) {
            // With no DBs, off-heap is effectively 0; treat it as included to avoid surprising UI/metrics.
            return emptyStats(instance.config().maxmemoryBytes(), true);
        }

        long heap = 0;
        long keyspaceOverhead = 0;
        long expireOverhead = 0;
        long expireValueObjects = 0;
        long offHeap = 0;
        long globalOffHeapFallback = 0;
        long reserved = 0;
        long usedBytesForMaxmemory = 0;
        long effectiveUsedBytesForMaxmemory = 0;
        int keyCount = 0;
        int expireCount = 0;
        boolean keysStoredOffHeap = false;
        boolean offHeapIncludedInMaxmemory = true;
        boolean keyspaceRehashing = false;
        boolean expireRehashing = false;
        int keyspaceCap0 = 0;
        int keyspaceCap1 = 0;
        int expireCap0 = 0;
        int expireCap1 = 0;
        long nativeDefragLastScannedObjects = 0;
        long nativeDefragLastMovedObjects = 0;
        long nativeDefragLastMovedBytes = 0;
        long nativeDefragLastSkippedPinnedObjects = 0;
        long nativeDefragLastSkippedBudgetObjects = 0;
        long nativeDefragLastFailedMoves = 0;
        long nativeDefragMovedBytes = 0;
        long nativeDefragSkippedPinnedObjects = 0;
        long nativeDefragQuarantinedObjects = 0;
        long nativeDefragQuarantineBytes = 0;
        long nativeStaleHandleDetections = 0;
        long nativeDefragReclaimedPages = 0;

        for (int dbIndex = 0; dbIndex < databases; dbIndex++) {
            YierdisMemoryStats s = instance.runtimeEngine(dbIndex).memory().memoryStats();
            heap += s.heapDataBytesEstimate();
            keyspaceOverhead += s.keyspaceTableOverheadBytesEstimate();
            expireOverhead += s.expireTableOverheadBytesEstimate();
            expireValueObjects += s.expireValueObjectsBytesEstimate();
            long dbOffHeap = Math.max(0L, s.offHeapUsedBytes());
            if (globalScope) {
                globalOffHeapFallback = addSaturating(globalOffHeapFallback, dbOffHeap);
            } else {
                // PER_DB: each DB owns its runtime, so sum.
                offHeap = addSaturating(offHeap, dbOffHeap);
            }
            reserved += s.reservedBytes();
            keyCount += s.keyCount();
            expireCount += s.expireCount();
            keysStoredOffHeap |= s.keysStoredOffHeap();
            keyspaceRehashing |= s.keyspaceRehashing();
            expireRehashing |= s.expireRehashing();
            keyspaceCap0 += s.keyspaceTable0Capacity();
            keyspaceCap1 += s.keyspaceTable1Capacity();
            expireCap0 += s.expireTable0Capacity();
            expireCap1 += s.expireTable1Capacity();
            nativeDefragLastScannedObjects += s.nativeDefragLastScannedObjects();
            nativeDefragLastMovedObjects += s.nativeDefragLastMovedObjects();
            nativeDefragLastMovedBytes += s.nativeDefragLastMovedBytes();
            nativeDefragLastSkippedPinnedObjects += s.nativeDefragLastSkippedPinnedObjects();
            nativeDefragLastSkippedBudgetObjects += s.nativeDefragLastSkippedBudgetObjects();
            nativeDefragLastFailedMoves += s.nativeDefragLastFailedMoves();
            nativeDefragMovedBytes += s.nativeDefragMovedBytes();
            nativeDefragSkippedPinnedObjects += s.nativeDefragSkippedPinnedObjects();
            nativeDefragQuarantinedObjects += s.nativeDefragQuarantinedObjects();
            nativeDefragQuarantineBytes += s.nativeDefragQuarantineBytes();
            nativeStaleHandleDetections += s.nativeStaleHandleDetections();
            nativeDefragReclaimedPages += s.nativeDefragReclaimedPages();
            if (!globalScope) {
                usedBytesForMaxmemory += s.usedBytesForMaxmemory();
                effectiveUsedBytesForMaxmemory += s.effectiveUsedBytesForMaxmemory();
                offHeapIncludedInMaxmemory &= s.offHeapIncludedInMaxmemory();
            }
        }

        if (globalScope) {
            // GLOBAL: DB participants exclude off-heap while the shared source counts actual native usage once.
            offHeap = globalOffHeapFallback;
            usedBytesForMaxmemory = addSaturating(heap, offHeap);
            effectiveUsedBytesForMaxmemory = addSaturating(usedBytesForMaxmemory, Math.max(0L, reserved));
            offHeapIncludedInMaxmemory = true;
        }

        long totalEstimatedBytes = addSaturating(
                addSaturating(addSaturating(heap, offHeap), keyspaceOverhead),
                addSaturating(expireOverhead, expireValueObjects)
        );

        return new YierdisMemoryStats(
                instance.config().maxmemoryBytes(),
                usedBytesForMaxmemory,
                heap,
                offHeap,
                reserved,
                effectiveUsedBytesForMaxmemory,
                offHeapIncludedInMaxmemory,
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
                nativeDefragLastScannedObjects,
                nativeDefragLastMovedObjects,
                nativeDefragLastMovedBytes,
                nativeDefragLastSkippedPinnedObjects,
                nativeDefragLastSkippedBudgetObjects,
                nativeDefragLastFailedMoves,
                nativeDefragMovedBytes,
                nativeDefragSkippedPinnedObjects,
                nativeDefragQuarantinedObjects,
                nativeDefragQuarantineBytes,
                nativeStaleHandleDetections,
                nativeDefragReclaimedPages
        );
    }

    public List<YierdisDbSummary> dbSummaries() {
        int databases = Math.max(0, instance.databases());
        if (databases == 0) {
            return List.of();
        }
        List<YierdisDbSummary> summaries = new ArrayList<>(databases);
        for (int dbIndex = 0; dbIndex < databases; dbIndex++) {
            YierdisMemoryStats s = instance.runtimeEngine(dbIndex).memory().memoryStats();
            summaries.add(new YierdisDbSummary(dbIndex, s.keyCount(), s.expireCount()));
        }
        return summaries;
    }

    private static YierdisMemoryStats emptyStats(long maxmemoryBytes, boolean offHeapIncludedInMaxmemory) {
        return new YierdisMemoryStats(
                maxmemoryBytes,
                0,
                0,
                0,
                0,
                0,
                offHeapIncludedInMaxmemory,
                false,
                0,
                0,
                false,
                0,
                0,
                0,
                false,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
        );
    }

    private static long addSaturating(long left, long right) {
        if (right <= 0) {
            return left;
        }
        if (left >= Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }
}
