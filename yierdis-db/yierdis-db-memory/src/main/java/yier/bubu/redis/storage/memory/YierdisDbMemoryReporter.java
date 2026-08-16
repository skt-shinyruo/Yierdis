package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
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
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;

import java.util.function.Supplier;

final class YierdisDbMemoryReporter {
    // 聚合 entry 派生状态、ledger 和 native allocator；物理快照仍只计入一次，不替代写路径的两阶段预算账本。
    private final YierdisDbKernel kernel;
    private final DbComponentMemoryUsage componentMemoryUsage;
    private final HashTableMaintenanceRegistry hashTableMaintenanceRegistry;
    private final long maxmemoryBytes;
    private final MemoryLedger ledger;
    private final YierdisDbMemoryEstimator memoryEstimator;
    private final Supplier<NativeDefragReport> nativeDefragReportSupplier;

    YierdisDbMemoryReporter(
            YierdisDbKernel kernel,
            DbComponentMemoryUsage componentMemoryUsage,
            HashTableMaintenanceRegistry hashTableMaintenanceRegistry,
            long maxmemoryBytes,
            MemoryLedger ledger,
            YierdisDbMemoryEstimator memoryEstimator,
            Supplier<NativeDefragReport> nativeDefragReportSupplier
    ) {
        this.kernel = java.util.Objects.requireNonNull(kernel, "kernel");
        this.componentMemoryUsage = java.util.Objects.requireNonNull(componentMemoryUsage, "componentMemoryUsage");
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
    }

    long memoryUsage(BytesView keyView) {
        return kernel.inspect(scope -> {
            KeyHandle keyHandle = scope.keyHandle(keyView);
            EntryRecord record = scope.liveEntryRecord(keyHandle);
            if (record == null) {
                return -1L;
            }
            long keyLen = Math.max(0L, (long) keyView.length());
            return metadataEstimatedBytes(keyHandle, record)
                    + estimateNativeBytesForMemoryUsage(scope, keyLen, record);
        });
    }

    long memoryUsage(byte[] keyBytes) {
        return kernel.inspect(scope -> {
            if (keyBytes == null) {
                return -1L;
            }
            KeyHandle keyHandle = scope.keyHandle(keyBytes);
            EntryRecord record = scope.liveEntryRecord(keyHandle);
            if (record == null) {
                return -1L;
            }
            return metadataEstimatedBytes(keyHandle, record)
                    + estimateNativeBytesForMemoryUsage(scope, keyBytes.length, record);
        });
    }

    YierdisMemoryStats memoryStats() {
        return kernel.inspect(scope -> DbMemoryAccounting.snapshot(
                maxmemoryBytes,
                componentMemoryUsage.snapshot(),
                ledger.reservedBytes(),
                scope.keyCount(),
                scope.expireCount(),
                hashTableMaintenanceRegistry,
                true,
                safeNativeAllocatorStats(scope),
                nativeDefragReportSupplier.get(),
                safeNativeLiveRegionCount(scope)
        ));
    }

    MemoryUsageSnapshot memoryUsage() {
        return kernel.inspect(ignored -> componentMemoryUsage.snapshot());
    }

    long usedBytesForMaxmemory() {
        return kernel.inspect(
                ignored -> componentMemoryUsage.snapshot().effectiveBytesForMaxmemory()
        );
    }

    long estimatedUsedBytes() {
        return usedBytesForMaxmemory();
    }

    int keyCountEstimate() {
        return kernel.inspect(scope -> {
            int size;
            try {
                size = scope.keyCount();
            } catch (Throwable ignored) {
                size = 0;
            }
            return Math.max(0, size);
        });
    }

    private long metadataEstimatedBytes(KeyHandle keyHandle, EntryRecord record) {
        if (record != null && keyHandle != null) {
            return memoryEstimator.estimateEntryBytes(keyHandle, record);
        }
        return 0L;
    }

    private long estimateNativeBytesForMemoryUsage(
            InspectionScope scope,
            long keyLen,
            EntryRecord record
    ) {
        if (record == null) {
            return 0;
        }
        long extra = 0;
        if (keyLen > 0) {
            extra += keyLen;
        }
        return MemoryUsageSnapshot.addSaturating(extra, scope.estimatedValueBytes(record));
    }

    private static NativeAllocatorStats safeNativeAllocatorStats(InspectionScope scope) {
        try {
            return scope.nativeAllocatorStats();
        } catch (Throwable ignored) {
            return null;
        }
    }

    private static long safeNativeLiveRegionCount(InspectionScope scope) {
        try {
            return Math.max(0L, scope.nativeLiveRegionCount());
        } catch (Throwable ignored) {
            return 0L;
        }
    }

}
