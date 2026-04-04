package yier.bubu.redis.runtime;

import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.ops.YierdisMemoryStats;

import java.util.Objects;

/**
 * Runtime-owned observability seam for instance-wide aggregation.
 */
public final class YierdisInstanceObservability {
    private final YierdisInstance instance;

    YierdisInstanceObservability(YierdisInstance instance) {
        this.instance = Objects.requireNonNull(instance, "instance");
    }

    public YierdisMemoryStats memoryStats() {
        DbEngine[] local = instance.engines();
        boolean globalScope = instance.config().maxmemoryScope() == YierdisInstanceConfig.MaxmemoryScope.GLOBAL;
        if (local.length == 0) {
            return emptyStats(instance.config().maxmemoryBytes(), globalScope);
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
        boolean keyspaceRehashing = false;
        boolean expireRehashing = false;
        int keyspaceCap0 = 0;
        int keyspaceCap1 = 0;
        int expireCap0 = 0;
        int expireCap1 = 0;

        for (DbEngine db : local) {
            if (db == null) {
                continue;
            }
            YierdisMemoryStats s = db.memory().memoryStats();
            heap += s.heapDataBytesEstimate();
            keyspaceOverhead += s.keyspaceTableOverheadBytesEstimate();
            expireOverhead += s.expireTableOverheadBytesEstimate();
            expireValueObjects += s.expireValueObjectsBytesEstimate();
            offHeap = Math.max(offHeap, s.offHeapUsedBytes());
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
            }
        }

        if (globalScope) {
            usedBytesForMaxmemory = heap + offHeap;
            effectiveUsedBytesForMaxmemory = usedBytesForMaxmemory + Math.max(0L, reserved);
        }

        long totalEstimatedBytes = heap + offHeap + keyspaceOverhead + expireOverhead + expireValueObjects;

        return new YierdisMemoryStats(
                instance.config().maxmemoryBytes(),
                usedBytesForMaxmemory,
                heap,
                offHeap,
                reserved,
                effectiveUsedBytesForMaxmemory,
                globalScope,
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
