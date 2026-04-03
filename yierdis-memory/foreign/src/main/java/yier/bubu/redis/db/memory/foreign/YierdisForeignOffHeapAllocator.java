package yier.bubu.redis.db.memory.foreign;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBackend;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBuf;
import yier.bubu.redis.db.memory.api.YierdisOffHeapOutOfMemoryException;
import yier.bubu.redis.db.memory.api.YierdisOffHeapSlice;
import yier.bubu.redis.bytes.BytesSource;
import yier.bubu.redis.offheap.api.OffHeapAllocator;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;

public final class YierdisForeignOffHeapAllocator implements OffHeapAllocator {
    private final long maxBytes;

    private boolean closed;
    private long usedBytes;

    public YierdisForeignOffHeapAllocator(long maxBytes) {
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must be >= 0");
        }
        this.maxBytes = maxBytes;
    }

    @Override
    public YierdisOffHeapBuf allocate(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        if (closed) {
            throw new IllegalStateException("allocator is closed");
        }

        long next = usedBytes + capacity;
        if (maxBytes > 0 && next > maxBytes) {
            throw new YierdisOffHeapOutOfMemoryException("off-heap memory limit exceeded");
        }

        Arena arena = Arena.ofConfined();
        MemorySegment segment = arena.allocate(capacity);
        usedBytes = next;
        return new YierdisForeignOffHeapBuf(this, arena, segment, capacity);
    }

    @Override
    public long usedBytes() {
        return usedBytes;
    }

    @Override
    public long maxBytes() {
        return maxBytes;
    }

    public YierdisOffHeapBackend backend() {
        return YierdisOffHeapBackend.FOREIGN;
    }

    @Override
    public void close() {
        closed = true;
        if (usedBytes != 0) {
            throw new IllegalStateException("off-heap leak: " + usedBytes + " bytes still allocated");
        }
    }

    void onFree(int capacity) {
        long next = usedBytes - capacity;
        if (next < 0) {
            throw new IllegalStateException("allocator accounting underflow");
        }
        usedBytes = next;
    }

    private static final class YierdisForeignOffHeapBuf implements YierdisOffHeapBuf {
        private static final int COPY_CHUNK_BYTES = 8 * 1024;
        private static final ThreadLocal<byte[]> TL_COPY_BUF =
                ThreadLocal.withInitial(() -> new byte[COPY_CHUNK_BYTES]);

        private final YierdisForeignOffHeapAllocator owner;
        private final Arena arena;
        private final MemorySegment segment;
        private final int capacity;

        private boolean closed;

        private YierdisForeignOffHeapBuf(
                YierdisForeignOffHeapAllocator owner,
                Arena arena,
                MemorySegment segment,
                int capacity
        ) {
            this.owner = owner;
            this.arena = arena;
            this.segment = segment;
            this.capacity = capacity;
        }

        @Override
        public int capacity() {
            return capacity;
        }

        @Override
        public byte getByte(int index) {
            ensureOpen();
            checkIndex(index, 1);
            return segment.get(ValueLayout.JAVA_BYTE, index);
        }

        @Override
        public void setByte(int index, byte value) {
            ensureOpen();
            checkIndex(index, 1);
            segment.set(ValueLayout.JAVA_BYTE, index, value);
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            ensureOpen();
            if (dst == null) {
                throw new IllegalArgumentException("dst must not be null");
            }
            if (len < 0) {
                throw new IllegalArgumentException("len must be >= 0");
            }
            checkIndex(index, len);
            if (dstOff < 0 || dstOff + len > dst.length) {
                throw new IndexOutOfBoundsException();
            }
            for (int i = 0; i < len; i++) {
                dst[dstOff + i] = segment.get(ValueLayout.JAVA_BYTE, index + i);
            }
        }

        @Override
        public void setBytes(int index, byte[] src, int srcOff, int len) {
            ensureOpen();
            if (src == null) {
                throw new IllegalArgumentException("src must not be null");
            }
            if (len < 0) {
                throw new IllegalArgumentException("len must be >= 0");
            }
            checkIndex(index, len);
            if (srcOff < 0 || srcOff + len > src.length) {
                throw new IndexOutOfBoundsException();
            }
            for (int i = 0; i < len; i++) {
                segment.set(ValueLayout.JAVA_BYTE, index + i, src[srcOff + i]);
            }
        }

        @Override
        public void setBytes(int index, BytesSource src, int srcIndex, int len) {
            ensureOpen();
            if (src == null) {
                throw new IllegalArgumentException("src must not be null");
            }
            if (len < 0) {
                throw new IllegalArgumentException("len must be >= 0");
            }
            checkIndex(index, len);
            if (srcIndex < 0) {
                throw new IndexOutOfBoundsException();
            }
            if (len == 0) {
                return;
            }

            ByteBuffer dst = segment.asSlice(index, len).asByteBuffer();
            byte[] scratch = TL_COPY_BUF.get();
            int remaining = len;
            int srcOff = srcIndex;
            while (remaining > 0) {
                int chunk = Math.min(remaining, scratch.length);
                src.getBytes(srcOff, scratch, 0, chunk);
                dst.put(scratch, 0, chunk);
                srcOff += chunk;
                remaining -= chunk;
            }
        }

        @Override
        public YierdisOffHeapSlice slice(int index, int len) {
            ensureOpen();
            if (len < 0) {
                throw new IllegalArgumentException("len must be >= 0");
            }
            checkIndex(index, len);
            return new YierdisForeignOffHeapSlice(this, index, len);
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            arena.close();
            owner.onFree(capacity);
        }

        private void ensureOpen() {
            if (closed) {
                throw new IllegalStateException("buffer is closed");
            }
            if (!arena.scope().isAlive()) {
                throw new IllegalStateException("arena is not alive");
            }
        }

        private void checkIndex(int index, int len) {
            if (index < 0 || index + len > capacity) {
                throw new IndexOutOfBoundsException();
            }
        }

        MemorySegment segment() {
            return segment;
        }

        void ensureOwnerOpen() {
            ensureOpen();
        }
    }

    private static final class YierdisForeignOffHeapSlice implements YierdisOffHeapSlice {
        private static final int COPY_CHUNK_BYTES = 8 * 1024;
        private static final ThreadLocal<byte[]> TL_COPY_BUF =
                ThreadLocal.withInitial(() -> new byte[COPY_CHUNK_BYTES]);

        private final YierdisForeignOffHeapBuf owner;
        private final int offset;
        private final int len;

        private YierdisForeignOffHeapSlice(YierdisForeignOffHeapBuf owner, int offset, int len) {
            this.owner = owner;
            this.offset = offset;
            this.len = len;
        }

        @Override
        public int length() {
            return len;
        }

        @Override
        public byte getByte(int index) {
            owner.ensureOwnerOpen();
            if (index < 0 || index >= len) {
                throw new IndexOutOfBoundsException();
            }
            return owner.segment().get(ValueLayout.JAVA_BYTE, offset + index);
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int readLen) {
            owner.ensureOwnerOpen();
            if (dst == null) {
                throw new IllegalArgumentException("dst must not be null");
            }
            if (readLen < 0) {
                throw new IllegalArgumentException("len must be >= 0");
            }
            if (index < 0 || index + readLen > len) {
                throw new IndexOutOfBoundsException();
            }
            if (dstOff < 0 || dstOff + readLen > dst.length) {
                throw new IndexOutOfBoundsException();
            }
            for (int i = 0; i < readLen; i++) {
                dst[dstOff + i] = owner.segment().get(ValueLayout.JAVA_BYTE, offset + index + i);
            }
        }

        @Override
        public void writeTo(BytesSink out) {
            owner.ensureOwnerOpen();
            if (out == null) {
                throw new IllegalArgumentException("out must not be null");
            }
            if (len == 0) {
                return;
            }

            MemorySegment s = owner.segment().asSlice(offset, len);
            ByteBuffer bb = s.asByteBuffer();

            byte[] scratch = TL_COPY_BUF.get();
            int remaining = len;
            while (remaining > 0) {
                int chunk = Math.min(remaining, scratch.length);
                bb.get(scratch, 0, chunk);
                out.writeBytes(scratch, 0, chunk);
                remaining -= chunk;
            }
        }
    }
}
