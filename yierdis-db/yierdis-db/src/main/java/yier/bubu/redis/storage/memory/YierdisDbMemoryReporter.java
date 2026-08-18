package yier.bubu.redis.storage.memory;

import java.util.Objects;
import java.util.function.Supplier;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.key.AllocatorKeyHandle;
import yier.bubu.redis.storage.memory.internal.ledger.DbMemoryAccounting;
import yier.bubu.redis.storage.memory.internal.ledger.MemoryLedger;

final class YierdisDbMemoryReporter {
    // 聚合 entry 派生状态、ledger 和 native allocator；物理快照仍只计入一次，不替代写路径的两阶段预算账本。
    private final YierdisDbKernel kernel;
    private final YierdisDbMemoryContext memoryContext;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final HashTableMaintenanceRegistry hashTableMaintenanceRegistry;
    private final long maxmemoryBytes;
    private final MemoryLedger ledger;
    private final Supplier<NativeDefragReport> nativeDefragReportSupplier;

    YierdisDbMemoryReporter(
            YierdisDbKernel kernel,
            YierdisDbMemoryContext memoryContext,
            YierdisDbKeyLifecycle keyLifecycle,
            HashTableMaintenanceRegistry hashTableMaintenanceRegistry,
            long maxmemoryBytes,
            MemoryLedger ledger,
            Supplier<NativeDefragReport> nativeDefragReportSupplier
    ) {
        this.kernel = Objects.requireNonNull(kernel, "kernel");
        this.memoryContext = Objects.requireNonNull(memoryContext, "memoryContext");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.hashTableMaintenanceRegistry = Objects.requireNonNull(
                hashTableMaintenanceRegistry,
                "hashTableMaintenanceRegistry"
        );
        this.maxmemoryBytes = maxmemoryBytes;
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.nativeDefragReportSupplier = Objects.requireNonNull(
                nativeDefragReportSupplier,
                "nativeDefragReportSupplier"
        );
    }

    long memoryUsage(BytesView keyView) {
        kernel.checkOwner();
        AllocatorKeyHandle keyHandle = keyLifecycle.keyHandle(keyView);
        EntryRecord record = kernel.liveEntryRecord(keyHandle);
        if (record == null) {
            return -1L;
        }
        long keyLen = Math.max(0L, (long) keyView.length());
        return metadataEstimatedBytes(keyHandle, record)
                + estimateNativeBytesForMemoryUsage(keyLen, record);
    }

    YierdisMemoryStats memoryStats() {
        kernel.checkOwner();
        return DbMemoryAccounting.snapshot(
                maxmemoryBytes,
                componentMemoryUsage(),
                ledger.reservedBytes(),
                keyLifecycle.keyCount(),
                keyLifecycle.expireCount(),
                hashTableMaintenanceRegistry,
                true,
                safeNativeAllocatorStats(),
                nativeDefragReportSupplier.get(),
                safeNativeLiveRegionCount()
        );
    }

    MemoryUsageSnapshot memoryUsage() {
        kernel.checkOwner();
        return componentMemoryUsage();
    }

    long usedBytesForMaxmemory() {
        kernel.checkOwner();
        return componentMemoryUsage().effectiveBytesForMaxmemory();
    }

    int keyCountEstimate() {
        kernel.checkOwner();
        try {
            return Math.max(0, keyLifecycle.keyCount());
        } catch (Throwable ignored) {
            return 0;
        }
    }

    private MemoryUsageSnapshot componentMemoryUsage() {
        MemoryUsageSnapshot retainedHeap = new MemoryUsageSnapshot(
                MemoryUsageSnapshot.addSaturating(
                        keyLifecycle.componentRetainedHeapBytes(),
                        hashTableMaintenanceRegistry.heapEstimatedBytes()
                ),
                0L,
                0L,
                0L,
                0L
        );
        return memoryContext.nativeMemoryUsage().plus(retainedHeap);
    }

    private long metadataEstimatedBytes(AllocatorKeyHandle keyHandle, EntryRecord record) {
        return record == null || keyHandle == null
                ? 0L
                : YierdisDbMemoryEstimator.entryMetadataBytes(record);
    }

    private long estimateNativeBytesForMemoryUsage(long keyLen, EntryRecord record) {
        if (record == null) {
            return 0L;
        }
        return MemoryUsageSnapshot.addSaturating(
                Math.max(0L, keyLen),
                keyLifecycle.estimatedValueBytes(record)
        );
    }

    private NativeAllocatorStats safeNativeAllocatorStats() {
        try {
            return memoryContext.nativeAllocatorStats();
        } catch (Throwable ignored) {
            return null;
        }
    }

    private long safeNativeLiveRegionCount() {
        try {
            return Math.max(0L, memoryContext.nativeLiveRegionCount());
        } catch (Throwable ignored) {
            return 0L;
        }
    }
}
