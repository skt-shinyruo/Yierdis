package yier.bubu.redis.memory.foreign;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import yier.bubu.redis.memory.api.NativeCapacityExceededException;

public final class YierdisFfmMemoryRuntime implements AutoCloseable {
    private final String name;
    private final AtomicLong usedBytes = new AtomicLong();
    private final AtomicLong liveRegionCount = new AtomicLong();

    private volatile boolean closed;

    public YierdisFfmMemoryRuntime(String name) {
        this.name = Objects.requireNonNull(name, "name");
    }

    public YierdisFfmRegion allocateRegion(String owner, int bytes) {
        if (bytes <= 0) {
            throw new IllegalArgumentException("bytes must be > 0");
        }
        if (closed) {
            throw new IllegalStateException("runtime is closed");
        }

        // Regions can be created during bootstrap and released on the DB owner thread, so they must be
        // closable from any thread.
        Arena arena = null;
        MemorySegment segment;
        try {
            arena = Arena.ofShared();
            segment = arena.allocate(bytes);
        } catch (OutOfMemoryError failure) {
            if (arena != null) {
                arena.close();
            }
            throw new NativeCapacityExceededException(
                    "native region allocation failed for " + bytes + " bytes",
                    failure
            );
        }
        YierdisFfmRegion region = new YierdisFfmRegion(this, owner, arena, segment, bytes);
        liveRegionCount.incrementAndGet();
        usedBytes.addAndGet(bytes);
        return region;
    }

    void onRegionClosed(YierdisFfmRegion region) {
        long regions = liveRegionCount.decrementAndGet();
        long bytes = usedBytes.addAndGet(-region.size());
        if (regions < 0 || bytes < 0) {
            throw new IllegalStateException("native memory runtime accounting underflow");
        }
    }

    public long usedBytes() {
        return usedBytes.get();
    }

    public long liveRegionCount() {
        return liveRegionCount.get();
    }

    @Override
    public void close() {
        closed = true;
        long regions = liveRegionCount.get();
        if (regions != 0) {
            throw new IllegalStateException("native memory leak in " + name + ": " + regions + " live regions");
        }
    }
}
