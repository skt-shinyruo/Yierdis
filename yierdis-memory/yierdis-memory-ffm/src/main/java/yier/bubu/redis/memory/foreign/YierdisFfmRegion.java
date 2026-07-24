package yier.bubu.redis.memory.foreign;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.Objects;
import yier.bubu.redis.memory.api.StableMemoryRegion;

final class YierdisFfmRegion implements StableMemoryRegion {
    private static final int COMPARE_CHUNK_BYTES = 8 * 1024;
    private static final ThreadLocal<byte[]> COMPARE_BUFFER =
            ThreadLocal.withInitial(() -> new byte[COMPARE_CHUNK_BYTES]);
    private static final ValueLayout.OfInt LITTLE_ENDIAN_INT =
            ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LITTLE_ENDIAN_LONG =
            ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
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

    @Override
    public int size() {
        ensureOpen();
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

    @Override
    public byte getByte(int offset) {
        ensureOpen();
        checkRange(offset, 1);
        return segment.get(ValueLayout.JAVA_BYTE, offset);
    }

    @Override
    public void setByte(int offset, byte value) {
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

    @Override
    public int getIntLittleEndian(int offset) {
        ensureOpen();
        checkRange(offset, Integer.BYTES);
        return segment.get(LITTLE_ENDIAN_INT, offset);
    }

    @Override
    public void setIntLittleEndian(int offset, int value) {
        ensureOpen();
        checkRange(offset, Integer.BYTES);
        segment.set(LITTLE_ENDIAN_INT, offset, value);
    }

    @Override
    public long getLongLittleEndian(int offset) {
        ensureOpen();
        checkRange(offset, Long.BYTES);
        return segment.get(LITTLE_ENDIAN_LONG, offset);
    }

    @Override
    public void setLongLittleEndian(int offset, long value) {
        ensureOpen();
        checkRange(offset, Long.BYTES);
        segment.set(LITTLE_ENDIAN_LONG, offset, value);
    }

    @Override
    public void getBytes(int offset, byte[] destination, int destinationOffset, int length) {
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

    @Override
    public void setBytes(int offset, byte[] source, int sourceOffset, int length) {
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

    void copyBytes(int sourceOffset, int targetOffset, int length) {
        ensureOpen();
        checkRange(sourceOffset, length);
        checkRange(targetOffset, length);
        MemorySegment.copy(segment, sourceOffset, segment, targetOffset, length);
    }

    @Override
    public void copyTo(
            int sourceOffset,
            StableMemoryRegion target,
            int targetOffset,
            int length
    ) {
        Objects.requireNonNull(target, "target");
        ensureOpen();
        checkRange(sourceOffset, length);
        if (target instanceof YierdisFfmRegion ffmTarget) {
            ffmTarget.ensureOpen();
            ffmTarget.checkRange(targetOffset, length);
            MemorySegment.copy(segment, sourceOffset, ffmTarget.segment, targetOffset, length);
            return;
        }
        byte[] snapshot = new byte[length];
        getBytes(sourceOffset, snapshot, 0, length);
        target.setBytes(targetOffset, snapshot, 0, length);
    }

    boolean contentEquals(int offset, byte[] other, int otherOffset, int length) {
        checkArrayRange(other, otherOffset, length, "other");
        ensureOpen();
        checkRange(offset, length);
        byte[] buffer = COMPARE_BUFFER.get();
        int compared = 0;
        while (compared < length) {
            int chunk = Math.min(buffer.length, length - compared);
            MemorySegment.copy(
                    segment,
                    ValueLayout.JAVA_BYTE,
                    offset + compared,
                    buffer,
                    0,
                    chunk
            );
            for (int index = 0; index < chunk; index++) {
                if (buffer[index] != other[otherOffset + compared + index]) {
                    return false;
                }
            }
            compared += chunk;
        }
        return true;
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
