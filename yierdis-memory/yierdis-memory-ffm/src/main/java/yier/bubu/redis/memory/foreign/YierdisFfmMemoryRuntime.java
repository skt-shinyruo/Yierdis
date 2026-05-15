package yier.bubu.redis.memory.foreign;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class YierdisFfmMemoryRuntime implements AutoCloseable {
    private final String name;
    private final AtomicLong usedBytes = new AtomicLong();
    private final Set<YierdisFfmRegion> liveRegions =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

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
        Arena arena = Arena.ofShared();
        MemorySegment segment = arena.allocate(bytes);
        YierdisFfmRegion region = new YierdisFfmRegion(this, owner, arena, segment, bytes);
        liveRegions.add(region);
        usedBytes.addAndGet(bytes);
        return region;
    }

    void onRegionClosed(YierdisFfmRegion region) {
        if (liveRegions.remove(region)) {
            usedBytes.addAndGet(-region.size());
        }
    }

    public long usedBytes() {
        return usedBytes.get();
    }

    @Override
    public void close() {
        closed = true;
        if (!liveRegions.isEmpty()) {
            throw new IllegalStateException("native memory leak in " + name + ": " + liveRegions.size() + " live regions");
        }
    }
}
