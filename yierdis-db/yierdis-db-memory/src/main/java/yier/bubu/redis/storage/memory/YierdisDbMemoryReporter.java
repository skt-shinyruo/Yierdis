package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.memory.internal.ledger.MemoryLedger;
import yier.bubu.redis.memory.api.OffHeapBuf;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.YierdisMemoryStats;

import java.util.function.BooleanSupplier;

public final class YierdisDbMemoryReporter {
    private final Runnable threadChecker;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final YierdisKeyspace<YierdisObject> store;
    private final YierdisExpireIndex expires;
    private final long maxmemoryBytes;
    private final boolean keysStoredOffHeap;
    private final MemoryLedger ledger;
    private final BooleanSupplier offHeapIncludedInMaxmemorySupplier;

    YierdisDbMemoryReporter(
            Runnable threadChecker,
            YierdisDbKeyLifecycle keyLifecycle,
            YierdisKeyspace<YierdisObject> store,
            YierdisExpireIndex expires,
            long maxmemoryBytes,
            boolean keysStoredOffHeap,
            MemoryLedger ledger,
            BooleanSupplier offHeapIncludedInMaxmemorySupplier
    ) {
        this.threadChecker = java.util.Objects.requireNonNull(threadChecker, "threadChecker");
        this.keyLifecycle = java.util.Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.store = java.util.Objects.requireNonNull(store, "store");
        this.expires = java.util.Objects.requireNonNull(expires, "expires");
        this.maxmemoryBytes = maxmemoryBytes;
        this.keysStoredOffHeap = keysStoredOffHeap;
        this.ledger = java.util.Objects.requireNonNull(ledger, "ledger");
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
                ledger.usedBytes(),
                ledger.reservedBytes(),
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
        long total = ledger.usedBytes() + nativeBytes;
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
        long entryBytesEstimate = yier.bubu.redis.storage.api.DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
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
