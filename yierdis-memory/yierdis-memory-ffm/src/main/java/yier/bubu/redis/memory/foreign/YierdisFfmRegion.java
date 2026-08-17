package yier.bubu.redis.memory.foreign;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

final class YierdisFfmRegion implements AutoCloseable {
    private final YierdisFfmMemoryRuntime runtime;
    private final Arena arena;
    private final MemorySegment segment;
    private final int size;

    private boolean closed;

    YierdisFfmRegion(
            YierdisFfmMemoryRuntime runtime,
            Arena arena,
            MemorySegment segment,
            int size
    ) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.arena = Objects.requireNonNull(arena, "arena");
        this.segment = Objects.requireNonNull(segment, "segment");
        if (size <= 0) {
            throw new IllegalArgumentException("size must be > 0");
        }
        this.size = size;
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
        ensureOpen();
        checkArrayRange(destination, destinationOffset, length, "destination");
        checkRange(offset, length);
        MemorySegment.copy(
                segment,
                ValueLayout.JAVA_BYTE,
                offset,
                destination,
                destinationOffset,
                length
        );
    }

    void setBytes(int offset, byte[] source, int sourceOffset, int length) {
        ensureOpen();
        checkArrayRange(source, sourceOffset, length, "source");
        checkRange(offset, length);
        MemorySegment.copy(
                source,
                sourceOffset,
                segment,
                ValueLayout.JAVA_BYTE,
                offset,
                length
        );
    }

    public void copyTo(
            int sourceOffset,
            YierdisFfmRegion target,
            int targetOffset,
            int length
    ) {
        Objects.requireNonNull(target, "target");
        ensureOpen();
        checkRange(sourceOffset, length);
        target.ensureOpen();
        target.checkRange(targetOffset, length);
        MemorySegment.copy(segment, sourceOffset, target.segment, targetOffset, length);
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
        int releasedBytes = size;
        RuntimeException failure = null;
        try {
            arena.close();
        } catch (RuntimeException closeFailure) {
            failure = closeFailure;
        }
        try {
            runtime.onRegionClosed(releasedBytes);
        } catch (RuntimeException accountingFailure) {
            if (failure == null) {
                failure = accountingFailure;
            } else {
                failure.addSuppressed(accountingFailure);
            }
        }
        if (failure != null) {
            throw failure;
        }
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
