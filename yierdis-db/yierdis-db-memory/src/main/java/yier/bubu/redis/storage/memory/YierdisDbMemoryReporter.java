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
import yier.bubu.redis.storage.memory.internal.entry.EntryTable;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.HashRoot;
import yier.bubu.redis.storage.memory.internal.entry.ListRoot;
import yier.bubu.redis.storage.memory.internal.entry.SetRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.entry.ZSetRoot;

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
                keyLifecycle.offHeapAllocator(),
                directNativeBytes(),
                store,
                expires,
                keysStoredOffHeap,
                offHeapIncludedInMaxmemorySupplier.getAsBoolean()
        );
    }

    long usedBytesForMaxmemory() {
        threadChecker.run();
        long nativeBytes = offHeapIncludedInMaxmemorySupplier.getAsBoolean() ? nativeBytesForMaxmemory() : 0L;
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
        if (type == ValueType.STRING && object.hasStringRoot()) {
            ValueHandle handle = record == null ? object.valueHandle() : record.valueHandle();
            if (handle != null) {
                extra += keyLifecycle.stringRoot().estimatedBytes(handle);
            }
            return extra;
        }
        if (type == ValueType.LIST && object.hasListRoot()) {
            ValueHandle handle = record == null ? object.valueHandle() : record.valueHandle();
            ListRoot listRoot = keyLifecycle.listRoot();
            if (handle != null && listRoot != null) {
                extra += listRoot.estimatedBytes(handle);
            }
            return extra;
        }
        if (type == ValueType.HASH && object.hasHashRoot()) {
            ValueHandle handle = record == null ? object.valueHandle() : record.valueHandle();
            HashRoot hashRoot = keyLifecycle.hashRoot();
            if (handle != null && hashRoot != null) {
                extra += hashRoot.estimatedBytes(handle);
            }
            return extra;
        }
        if (type == ValueType.SET && object.hasSetRoot()) {
            ValueHandle handle = record == null ? object.valueHandle() : record.valueHandle();
            SetRoot setRoot = keyLifecycle.setRoot();
            if (handle != null && setRoot != null) {
                extra += setRoot.estimatedBytes(handle);
            }
            return extra;
        }
        if (type == ValueType.ZSET && object.hasZSetRoot()) {
            ValueHandle handle = record == null ? object.valueHandle() : record.valueHandle();
            ZSetRoot zsetRoot = keyLifecycle.zsetRoot();
            if (handle != null && zsetRoot != null) {
                extra += zsetRoot.estimatedBytes(handle);
            }
            return extra;
        }
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

    private long nativeBytesForMaxmemory() {
        return addSaturating(safeOffHeapUsedBytes(), directNativeBytes());
    }

    private long directNativeBytes() {
        long total = 0L;
        if (store instanceof YierdisFfmKeyspace<?> ffmStore) {
            total = addSaturating(total, ffmStore.nativeBytes());
        }
        if (expires instanceof YierdisFfmExpireIndex ffmExpires) {
            total = addSaturating(total, ffmExpires.nativeBytes());
        }
        EntryTable entryTable = keyLifecycle.entryTable();
        if (entryTable != null) {
            total = addSaturating(total, entryTable.nativeBytes());
        }
        NativeKeyDirectory keyDirectory = keyLifecycle.keyDirectory();
        if (keyDirectory != null) {
            total = addSaturating(total, keyDirectory.nativeBytes());
        }
        ListRoot listRoot = keyLifecycle.listRoot();
        if (listRoot != null) {
            total = addSaturating(total, listRoot.nativeBytes());
        }
        HashRoot hashRoot = keyLifecycle.hashRoot();
        if (hashRoot != null) {
            total = addSaturating(total, hashRoot.nativeBytes());
        }
        SetRoot setRoot = keyLifecycle.setRoot();
        if (setRoot != null) {
            total = addSaturating(total, setRoot.nativeBytes());
        }
        ZSetRoot zsetRoot = keyLifecycle.zsetRoot();
        if (zsetRoot != null) {
            total = addSaturating(total, zsetRoot.nativeBytes());
        }
        return total;
    }

    private long safeOffHeapUsedBytes() {
        var allocator = keyLifecycle.offHeapAllocator();
        if (allocator == null) {
            return 0L;
        }
        try {
            return Math.max(0L, allocator.usedBytes());
        } catch (Throwable ignored) {
            return 0L;
        }
    }

    private static long addSaturating(long left, long right) {
        if (left < 0 || right < 0) {
            return Long.MAX_VALUE;
        }
        if (Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }
}
