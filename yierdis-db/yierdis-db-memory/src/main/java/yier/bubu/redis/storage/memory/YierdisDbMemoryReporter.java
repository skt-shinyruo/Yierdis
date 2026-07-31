package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.ledger.MemoryLedger;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.HashRoot;
import yier.bubu.redis.storage.memory.internal.entry.ListRoot;
import yier.bubu.redis.storage.memory.internal.entry.SetRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.entry.ZSetRoot;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

public final class YierdisDbMemoryReporter {
    // 聚合 entry 派生状态、ledger 和 native allocator；物理快照仍只计入一次，不替代写路径的两阶段预算账本。
    private final YierdisDbRuntimeInternals internals;
    private final DbComponentMemoryUsage componentMemoryUsage;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final HashTableMaintenanceRegistry hashTableMaintenanceRegistry;
    private final long maxmemoryBytes;
    private final MemoryLedger ledger;
    private final YierdisDbMemoryEstimator memoryEstimator;
    private final Supplier<NativeDefragReport> nativeDefragReportSupplier;
    private final LongSupplier nativeLiveRegionCountSupplier;

    YierdisDbMemoryReporter(
            YierdisDbRuntimeInternals internals,
            DbComponentMemoryUsage componentMemoryUsage,
            HashTableMaintenanceRegistry hashTableMaintenanceRegistry,
            long maxmemoryBytes,
            MemoryLedger ledger,
            YierdisDbMemoryEstimator memoryEstimator,
            Supplier<NativeDefragReport> nativeDefragReportSupplier,
            LongSupplier nativeLiveRegionCountSupplier
    ) {
        this.internals = java.util.Objects.requireNonNull(internals, "internals");
        this.componentMemoryUsage = java.util.Objects.requireNonNull(componentMemoryUsage, "componentMemoryUsage");
        this.keyLifecycle = internals.keyLifecycle();
        this.hashTableMaintenanceRegistry = java.util.Objects.requireNonNull(
                hashTableMaintenanceRegistry,
                "hashTableMaintenanceRegistry"
        );
        this.maxmemoryBytes = maxmemoryBytes;
        this.ledger = java.util.Objects.requireNonNull(ledger, "ledger");
        this.memoryEstimator = java.util.Objects.requireNonNull(memoryEstimator, "memoryEstimator");
        this.nativeDefragReportSupplier = java.util.Objects.requireNonNull(
                nativeDefragReportSupplier,
                "nativeDefragReportSupplier"
        );
        this.nativeLiveRegionCountSupplier = java.util.Objects.requireNonNull(
                nativeLiveRegionCountSupplier,
                "nativeLiveRegionCountSupplier"
        );
    }

    long memoryUsage(BytesView keyView) {
        internals.checkThread();
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyView);
        EntryRecord record = liveEntryRecord(keyHandle);
        if (record == null) {
            return -1;
        }
        long keyLen = keyView == null ? 0 : Math.max(0L, (long) keyView.length());
        return metadataEstimatedBytes(keyHandle, record) + estimateNativeBytesForMemoryUsage(keyLen, record);
    }

    long memoryUsage(byte[] keyBytes) {
        internals.checkThread();
        if (keyBytes == null) {
            return -1;
        }
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        EntryRecord record = liveEntryRecord(keyHandle);
        if (record == null) {
            return -1;
        }
        return metadataEstimatedBytes(keyHandle, record) + estimateNativeBytesForMemoryUsage(keyBytes.length, record);
    }

    YierdisMemoryStats memoryStats() {
        internals.checkThread();
        MemoryUsageSnapshot usage = memoryUsage();
        return DbMemoryAccounting.snapshot(
                maxmemoryBytes,
                usage,
                ledger.reservedBytes(),
                keyLifecycle.keyCount(),
                keyLifecycle.expireCount(),
                keyLifecycle.expiredEntriesAwaitingPhysicalDeletion(),
                hashTableMaintenanceRegistry,
                true,
                safeNativeAllocatorStats(),
                nativeDefragReportSupplier.get(),
                safeNativeLiveRegionCount()
        );
    }

    MemoryUsageSnapshot memoryUsage() {
        internals.checkThread();
        return componentMemoryUsage.snapshot();
    }

    long usedBytesForMaxmemory() {
        internals.checkThread();
        return memoryUsage().effectiveBytesForMaxmemory();
    }

    long estimatedUsedBytes() {
        return usedBytesForMaxmemory();
    }

    int keyCountEstimate() {
        internals.checkThread();
        int size;
        try {
            size = keyLifecycle.keyCount();
        } catch (Throwable ignored) {
            size = 0;
        }
        return Math.max(0, size);
    }

    private EntryRecord liveEntryRecord(KeyHandle keyHandle) {
        return internals.liveEntryRecord(keyHandle);
    }

    private long metadataEstimatedBytes(KeyHandle keyHandle, EntryRecord record) {
        if (record != null && keyHandle != null) {
            return memoryEstimator.estimateEntryBytes(keyHandle, record);
        }
        return 0L;
    }

    private long estimateNativeBytesForMemoryUsage(long keyLen, EntryRecord record) {
        if (record == null) {
            return 0;
        }
        long extra = 0;
        if (keyLen > 0) {
            extra += keyLen;
        }
        ValueType type = record.type();
        if (type == ValueType.STRING) {
            ValueHandle handle = record.valueHandle();
            if (handle != null) {
                extra += keyLifecycle.stringRoot().estimatedBytes(handle);
            }
            return extra;
        }
        if (type == ValueType.LIST) {
            ValueHandle handle = record.valueHandle();
            ListRoot listRoot = keyLifecycle.listRoot();
            if (handle != null && listRoot != null) {
                extra += listRoot.estimatedBytes(handle);
            }
            return extra;
        }
        if (type == ValueType.HASH) {
            ValueHandle handle = record.valueHandle();
            HashRoot hashRoot = keyLifecycle.hashRoot();
            if (handle != null && hashRoot != null) {
                extra += hashRoot.estimatedBytes(handle);
            }
            return extra;
        }
        if (type == ValueType.SET) {
            ValueHandle handle = record.valueHandle();
            SetRoot setRoot = keyLifecycle.setRoot();
            if (handle != null && setRoot != null) {
                extra += setRoot.estimatedBytes(handle);
            }
            return extra;
        }
        if (type == ValueType.ZSET) {
            ValueHandle handle = record.valueHandle();
            ZSetRoot zsetRoot = keyLifecycle.zsetRoot();
            if (handle != null && zsetRoot != null) {
                extra += zsetRoot.estimatedBytes(handle);
            }
            return extra;
        }
        return extra;
    }

    private NativeAllocatorStats safeNativeAllocatorStats() {
        var allocator = keyLifecycle.stableMemoryBackend();
        if (allocator == null) {
            return null;
        }
        try {
            return allocator.stats();
        } catch (Throwable ignored) {
            return null;
        }
    }

    private long safeNativeLiveRegionCount() {
        try {
            return Math.max(0L, nativeLiveRegionCountSupplier.getAsLong());
        } catch (Throwable ignored) {
            return 0L;
        }
    }

}
