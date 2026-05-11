package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.ledger.MemoryLedger;
import yier.bubu.redis.memory.api.OffHeapBuf;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;

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
    private final YierdisDbMemoryEstimator memoryEstimator;

    YierdisDbMemoryReporter(
            Runnable threadChecker,
            YierdisDbKeyLifecycle keyLifecycle,
            YierdisKeyspace<YierdisObject> store,
            YierdisExpireIndex expires,
            long maxmemoryBytes,
            boolean keysStoredOffHeap,
            MemoryLedger ledger,
            BooleanSupplier offHeapIncludedInMaxmemorySupplier,
            YierdisDbMemoryEstimator memoryEstimator
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
        this.memoryEstimator = java.util.Objects.requireNonNull(memoryEstimator, "memoryEstimator");
    }

    long memoryUsage(BytesView keyView) {
        threadChecker.run();
        EntryRecord record = keyLifecycle.liveEntryRecord(keyView);
        YierdisObject object = keyLifecycle.getLiveObject(keyView);
        if (object == null) {
            return -1;
        }
        long keyLen = keyView == null ? 0 : Math.max(0L, (long) keyView.len());
        var keyHandle = keyLifecycle.keyHandle(keyView);
        return metadataEstimatedBytes(keyHandle, record, object) + estimateOffHeapBytesForMemoryUsage(keyLen, record, object);
    }

    long memoryUsage(byte[] keyBytes) {
        threadChecker.run();
        if (keyBytes == null) {
            return -1;
        }
        EntryRecord record = keyLifecycle.liveEntryRecord(keyBytes);
        YierdisObject object = keyLifecycle.getLiveObject(keyBytes);
        if (object == null) {
            return -1;
        }
        var keyHandle = keyLifecycle.keyHandle(keyBytes);
        return metadataEstimatedBytes(keyHandle, record, object) + estimateOffHeapBytesForMemoryUsage(keyBytes.length, record, object);
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

    private long metadataEstimatedBytes(KeyHandle keyHandle, EntryRecord record, YierdisObject object) {
        if (record != null && keyHandle != null) {
            return memoryEstimator.estimateEntryBytes(keyHandle, record);
        }
        return object == null ? 0L : object.estimatedBytes;
    }

    private long estimateOffHeapBytesForMemoryUsage(long keyLen, EntryRecord record, YierdisObject object) {
        if (keyLifecycle.offHeapAllocator() == null || object == null) {
            return 0;
        }
        long extra = 0;
        if (keysStoredOffHeap && keyLen > 0) {
            extra += keyLen;
        }
        ValueType type = record == null ? object.type : record.type();
        if (type == ValueType.STRING && object.payload instanceof OffHeapBuf buf) {
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
