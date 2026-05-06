package yier.bubu.redis.db.memory.foreign;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Objects;

public final class YierdisFfmRegion implements AutoCloseable {
    private final YierdisFfmMemoryRuntime runtime;
    private final String owner;
    private final Arena arena;
    private final MemorySegment segment;
    private final int size;

    private boolean closed;

    YierdisFfmRegion(
            YierdisFfmMemoryRuntime runtime,
            String owner,
            Arena arena,
            MemorySegment segment,
            int size
    ) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.arena = Objects.requireNonNull(arena, "arena");
        this.segment = Objects.requireNonNull(segment, "segment");
        if (size <= 0) {
            throw new IllegalArgumentException("size must be > 0");
        }
        this.size = size;
    }

    public int size() {
        return size;
    }

    public String owner() {
        return owner;
    }

    public YierdisFfmSpan span(int offset, int length) {
        ensureOpen();
        checkRange(offset, length);
        return new YierdisFfmSpan(segment.asSlice(offset, length));
    }

    void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("region is closed");
        }
        if (!arena.scope().isAlive()) {
            throw new IllegalStateException("arena is not alive");
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        arena.close();
        runtime.onRegionClosed(this);
    }

    private void checkRange(int offset, int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0");
        }
        if (offset < 0 || offset + length > size) {
            throw new IndexOutOfBoundsException();
        }
    }
}
