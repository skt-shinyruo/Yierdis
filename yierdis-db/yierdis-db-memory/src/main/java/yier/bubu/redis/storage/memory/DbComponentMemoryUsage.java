package yier.bubu.redis.storage.memory;

import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;

import java.util.Objects;

final class DbComponentMemoryUsage {
    private final Runnable threadChecker;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final HashTableMaintenanceRegistry hashTableMaintenanceRegistry;

    DbComponentMemoryUsage(
            Runnable threadChecker,
            YierdisDbKeyLifecycle keyLifecycle,
            HashTableMaintenanceRegistry hashTableMaintenanceRegistry
    ) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.hashTableMaintenanceRegistry = Objects.requireNonNull(
                hashTableMaintenanceRegistry,
                "hashTableMaintenanceRegistry"
        );
    }

    MemoryUsageSnapshot snapshot() {
        threadChecker.run();
        MemoryUsageSnapshot usage = keyLifecycle.stableMemoryBackend().memoryUsage();
        MemoryUsageSnapshot retainedHeap = new MemoryUsageSnapshot(
                MemoryUsageSnapshot.addSaturating(
                        keyLifecycle.keyDirectory().heapBytes(),
                        MemoryUsageSnapshot.addSaturating(
                                keyLifecycle.listRoot().heapBytes(),
                                MemoryUsageSnapshot.addSaturating(
                                        keyLifecycle.hashRoot().heapBytes(),
                                        MemoryUsageSnapshot.addSaturating(
                                                keyLifecycle.setRoot().heapBytes(),
                                                MemoryUsageSnapshot.addSaturating(
                                                        keyLifecycle.zsetRoot().heapBytes(),
                                                        hashTableMaintenanceRegistry.heapEstimatedBytes()
                                                )
                                        )
                                )
                        )
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
