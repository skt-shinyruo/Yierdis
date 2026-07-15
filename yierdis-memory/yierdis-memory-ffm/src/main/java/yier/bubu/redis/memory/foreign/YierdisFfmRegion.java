package yier.bubu.redis.memory.foreign;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
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

    byte getByte(int offset) {
        ensureOpen();
        checkRange(offset, 1);
        return segment.get(ValueLayout.JAVA_BYTE, offset);
    }

    void setByte(int offset, byte value) {
        ensureOpen();
        checkRange(offset, 1);
        segment.set(ValueLayout.JAVA_BYTE, offset, value);
    }

    long getLong(int offset) {
        ensureOpen();
        checkRange(offset, Long.BYTES);
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
    }

    void setLong(int offset, long value) {
        ensureOpen();
        checkRange(offset, Long.BYTES);
        segment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, value);
    }

    int getInt(int offset) {
        ensureOpen();
        checkRange(offset, Integer.BYTES);
        return segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
    }

    void setInt(int offset, int value) {
        ensureOpen();
        checkRange(offset, Integer.BYTES);
        segment.set(ValueLayout.JAVA_INT_UNALIGNED, offset, value);
    }

    void getBytes(int offset, byte[] destination, int destinationOffset, int length) {
        checkArrayRange(destination, destinationOffset, length, "destination");
        ensureOpen();
        checkRange(offset, length);
        for (int index = 0; index < length; index++) {
            destination[destinationOffset + index] = segment.get(ValueLayout.JAVA_BYTE, offset + index);
        }
    }

    void setBytes(int offset, byte[] source, int sourceOffset, int length) {
        checkArrayRange(source, sourceOffset, length, "source");
        ensureOpen();
        checkRange(offset, length);
        for (int index = 0; index < length; index++) {
            segment.set(ValueLayout.JAVA_BYTE, offset + index, source[sourceOffset + index]);
        }
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
        if (offset < 0 || offset > size - length) {
            throw new IndexOutOfBoundsException();
        }
    }

    private static void checkArrayRange(byte[] array, int offset, int length, String name) {
        if (array == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0");
        }
        if (offset < 0 || offset > array.length - length) {
            throw new IndexOutOfBoundsException();
        }
    }
}
