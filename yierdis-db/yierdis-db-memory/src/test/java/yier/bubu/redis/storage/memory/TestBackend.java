package yier.bubu.redis.storage.memory;

import java.util.Objects;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.MemoryOwner;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.StableMemoryBackendFactory;
import yier.bubu.redis.memory.api.StableMemoryRegion;
import yier.bubu.redis.memory.testkit.HeapStableMemoryBackend;

/** 测试中显式绑定的后端资源；关闭时只负责释放后端本身。 */
public final class TestBackend implements StableMemoryBackendFactory, AutoCloseable {
    private final String name;
    private final int maxSlots;
    private HeapStableMemoryBackend backend;

    private TestBackend(String name, int maxSlots) {
        this.name = Objects.requireNonNull(name, "name");
        if (maxSlots < 0) {
            throw new IllegalArgumentException("maxSlots must be non-negative");
        }
        this.maxSlots = maxSlots;
    }

    public static TestBackend open(String name, int maxSlots) {
        return new TestBackend(name, maxSlots);
    }

    public static TestBackend open(String name) {
        return new TestBackend(name, 0);
    }

    public StableMemoryBackend backend() {
        return heap();
    }

    public HeapStableMemoryBackend heap() {
        if (backend == null) {
            backend = new HeapStableMemoryBackend(name, maxSlots, new DbThreadGuard());
            backend.bindToCurrentThread();
        }
        return backend;
    }

    int maxSlots() {
        return maxSlots;
    }

    @Override
    public StableMemoryBackend create(String ignoredName, int requestedMaxSlots, MemoryOwner owner) {
        if (backend != null) {
            throw new IllegalStateException("test backend has already been created");
        }
        backend = new HeapStableMemoryBackend(name, requestedMaxSlots, Objects.requireNonNull(owner, "owner"));
        return backend;
    }

    public long usedBytes() {
        MemoryUsageSnapshot usage = heap().memoryUsage();
        return usage.effectiveBytesForMaxmemory();
    }

    public long liveRegionCount() {
        return heap().liveRegionCount();
    }

    public StableMemoryRegion allocateRegion(String owner, int bytes) {
        return heap().allocateRegion(owner, bytes);
    }

    @Override
    public void close() {
        if (backend != null) {
            backend.close();
        }
    }
}
