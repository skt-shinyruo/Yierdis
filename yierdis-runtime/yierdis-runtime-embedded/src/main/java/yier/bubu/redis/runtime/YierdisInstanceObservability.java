package yier.bubu.redis.runtime;

import yier.bubu.redis.ops.YierdisMemoryStats;

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

        for (int dbIndex = 0; dbIndex < databases; dbIndex++) {
            YierdisMemoryStats s = instance.runtimeEngine(dbIndex).memory().memoryStats();
            heap += s.heapDataBytesEstimate();
            keyspaceOverhead += s.keyspaceTableOverheadBytesEstimate();
            expireOverhead += s.expireTableOverheadBytesEstimate();
            expireValueObjects += s.expireValueObjectsBytesEstimate();
            long dbOffHeap = Math.max(0L, s.offHeapUsedBytes());
            if (globalScope) {
                // GLOBAL: the default wiring uses a shared off-heap runtime, so count it once.
                offHeap = Math.max(offHeap, dbOffHeap);
            } else {
                // PER_DB: each DB owns its runtime, so sum.
                if (Long.MAX_VALUE - offHeap < dbOffHeap) {
                    offHeap = Long.MAX_VALUE;
                } else {
                    offHeap += dbOffHeap;
                }
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
            if (!globalScope) {
                usedBytesForMaxmemory += s.usedBytesForMaxmemory();
                effectiveUsedBytesForMaxmemory += s.effectiveUsedBytesForMaxmemory();
                offHeapIncludedInMaxmemory &= s.offHeapIncludedInMaxmemory();
            }
        }

        if (globalScope) {
            usedBytesForMaxmemory = heap + offHeap;
            effectiveUsedBytesForMaxmemory = usedBytesForMaxmemory + Math.max(0L, reserved);
            offHeapIncludedInMaxmemory = true;
        }

        long totalEstimatedBytes = heap + offHeap + keyspaceOverhead + expireOverhead + expireValueObjects;

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
                totalEstimatedBytes
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
                0
        );
    }
}
