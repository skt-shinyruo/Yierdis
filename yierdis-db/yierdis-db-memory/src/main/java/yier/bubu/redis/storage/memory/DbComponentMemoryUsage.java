package yier.bubu.redis.storage.memory;

import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;

import java.util.Objects;

final class DbComponentMemoryUsage {
    private final Runnable threadChecker;
    private final YierdisDbMemoryContext memoryContext;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final HashTableMaintenanceRegistry hashTableMaintenanceRegistry;

    DbComponentMemoryUsage(
            Runnable threadChecker,
            YierdisDbMemoryContext memoryContext,
            YierdisDbKeyLifecycle keyLifecycle,
            HashTableMaintenanceRegistry hashTableMaintenanceRegistry
    ) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.memoryContext = Objects.requireNonNull(memoryContext, "memoryContext");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.hashTableMaintenanceRegistry = Objects.requireNonNull(
                hashTableMaintenanceRegistry,
                "hashTableMaintenanceRegistry"
        );
    }

    MemoryUsageSnapshot snapshot() {
        threadChecker.run();
        MemoryUsageSnapshot usage = memoryContext.nativeMemoryUsage();
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
        usage = usage.plus(retainedHeap);
        return usage;
    }
}
