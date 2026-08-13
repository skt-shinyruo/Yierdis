package yier.bubu.redis.storage.memory;

import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;

import java.util.Objects;

final class DbComponentMemoryUsage {
    private final Runnable threadChecker;
    private final YierdisDbRuntimeInternals internals;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final HashTableMaintenanceRegistry hashTableMaintenanceRegistry;

    DbComponentMemoryUsage(
            Runnable threadChecker,
            YierdisDbRuntimeInternals internals,
            YierdisDbKeyLifecycle keyLifecycle,
            HashTableMaintenanceRegistry hashTableMaintenanceRegistry
    ) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.internals = Objects.requireNonNull(internals, "internals");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.hashTableMaintenanceRegistry = Objects.requireNonNull(
                hashTableMaintenanceRegistry,
                "hashTableMaintenanceRegistry"
        );
    }

    MemoryUsageSnapshot snapshot() {
        threadChecker.run();
        MemoryUsageSnapshot usage = internals.nativeMemoryUsage();
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
