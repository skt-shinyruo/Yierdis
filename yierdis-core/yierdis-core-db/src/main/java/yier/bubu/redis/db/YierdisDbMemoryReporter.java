package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.offheap.api.OffHeapBuf;
import yier.bubu.redis.ops.ValueType;
import yier.bubu.redis.ops.YierdisMemoryStats;

import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

final class YierdisDbMemoryReporter {
    private final Runnable threadChecker;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final YierdisKeyspace<YierdisObject> store;
    private final YierdisExpireIndex expires;
    private final long maxmemoryBytes;
    private final boolean keysStoredOffHeap;
    private final LongSupplier usedBytesSupplier;
    private final LongSupplier reservedBytesSupplier;
    private final BooleanSupplier offHeapIncludedInMaxmemorySupplier;

    YierdisDbMemoryReporter(
            Runnable threadChecker,
            YierdisDbKeyLifecycle keyLifecycle,
            YierdisKeyspace<YierdisObject> store,
            YierdisExpireIndex expires,
            long maxmemoryBytes,
            boolean keysStoredOffHeap,
            LongSupplier usedBytesSupplier,
            LongSupplier reservedBytesSupplier,
            BooleanSupplier offHeapIncludedInMaxmemorySupplier
    ) {
        this.threadChecker = java.util.Objects.requireNonNull(threadChecker, "threadChecker");
        this.keyLifecycle = java.util.Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.store = java.util.Objects.requireNonNull(store, "store");
        this.expires = java.util.Objects.requireNonNull(expires, "expires");
        this.maxmemoryBytes = maxmemoryBytes;
        this.keysStoredOffHeap = keysStoredOffHeap;
        this.usedBytesSupplier = java.util.Objects.requireNonNull(usedBytesSupplier, "usedBytesSupplier");
        this.reservedBytesSupplier = java.util.Objects.requireNonNull(reservedBytesSupplier, "reservedBytesSupplier");
        this.offHeapIncludedInMaxmemorySupplier = java.util.Objects.requireNonNull(
                offHeapIncludedInMaxmemorySupplier,
                "offHeapIncludedInMaxmemorySupplier"
        );
    }

    long memoryUsage(BytesView keyView) {
        threadChecker.run();
        YierdisObject object = keyLifecycle.getLiveObject(keyView);
        if (object == null) {
            return -1;
        }
        long keyLen = keyView == null ? 0 : Math.max(0L, (long) keyView.len());
        return object.estimatedBytes + estimateOffHeapBytesForMemoryUsage(keyLen, object);
    }

    long memoryUsage(byte[] keyBytes) {
        threadChecker.run();
        if (keyBytes == null) {
            return -1;
        }
        YierdisObject object = keyLifecycle.getLiveObject(keyBytes);
        if (object == null) {
            return -1;
        }
        return object.estimatedBytes + estimateOffHeapBytesForMemoryUsage(keyBytes.length, object);
    }

    YierdisMemoryStats memoryStats() {
        threadChecker.run();
        return DbMemoryAccounting.snapshot(
                maxmemoryBytes,
                usedBytesSupplier.getAsLong(),
                reservedBytesSupplier.getAsLong(),
                null,
                runtimeUsedBytes(),
                store,
                expires,
                keysStoredOffHeap,
                offHeapIncludedInMaxmemorySupplier.getAsBoolean()
        );
    }

    long usedBytesForMaxmemory() {
        threadChecker.run();
        long nativeBytes = offHeapIncludedInMaxmemorySupplier.getAsBoolean() ? runtimeUsedBytes() : 0L;
        long ttlBytes = estimateTtlBytesForMaxmemory();
        long total = usedBytesSupplier.getAsLong() + nativeBytes;
        if (ttlBytes <= 0) {
            return total;
        }
        if (Long.MAX_VALUE - total < ttlBytes) {
            return Long.MAX_VALUE;
        }
        return total + ttlBytes;
    }

    long estimatedUsedBytes() {
        return usedBytesForMaxmemory();
    }

    int keyCountEstimate() {
        threadChecker.run();
        int size;
        try {
            size = store.size();
        } catch (Throwable ignored) {
            size = 0;
        }
        return Math.max(0, size);
    }

    private long estimateOffHeapBytesForMemoryUsage(long keyLen, YierdisObject object) {
        if (keyLifecycle.offHeapAllocator() == null || object == null) {
            return 0;
        }
        long extra = 0;
        if (keysStoredOffHeap && keyLen > 0) {
            extra += keyLen;
        }
        if (object.type == ValueType.STRING && object.payload instanceof OffHeapBuf buf) {
            extra += buf.capacity();
        }
        return extra;
    }

    private long estimateTtlBytesForMaxmemory() {
        long entryBytesEstimate = yier.bubu.redis.ops.DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
        if (entryBytesEstimate <= 0) {
            return 0;
        }
        int ttlCount;
        try {
            ttlCount = expires.size();
        } catch (Throwable ignored) {
            ttlCount = 0;
        }
        if (ttlCount <= 0) {
            return 0;
        }
        try {
            return Math.multiplyExact((long) ttlCount, entryBytesEstimate);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private long runtimeUsedBytes() {
        var memoryRuntime = keyLifecycle.memoryRuntime();
        if (memoryRuntime == null) {
            return 0L;
        }
        try {
            return Math.max(0L, memoryRuntime.usedBytes());
        } catch (Throwable ignored) {
            return 0L;
        }
    }
}
