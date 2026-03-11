package yier.bubu.redis.db.offheap.foreign;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBackend;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBuf;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapOutOfMemoryException;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapSlice;
import yier.bubu.redis.bytes.BytesSource;

import java.nio.ByteBuffer;

public final class YierdisForeignOffHeapAllocator implements YierdisOffHeapAllocator {
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

        ResourceScope scope = ResourceScope.newConfinedScope();
        MemorySegment segment = MemorySegment.allocateNative(capacity, scope);
        usedBytes = next;
        return new YierdisForeignOffHeapBuf(this, scope, segment, capacity);
    }

    @Override
    public long usedBytes() {
        return usedBytes;
    }

    @Override
    public long maxBytes() {
        return maxBytes;
    }

    @Override
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
        private final ResourceScope scope;
        private final MemorySegment segment;
        private final int capacity;

        private boolean closed;

        private YierdisForeignOffHeapBuf(
                YierdisForeignOffHeapAllocator owner,
                ResourceScope scope,
                MemorySegment segment,
                int capacity
        ) {
            this.owner = owner;
            this.scope = scope;
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
            return MemoryAccess.getByteAtOffset(segment, index);
        }

        @Override
        public void setByte(int index, byte value) {
            ensureOpen();
            checkIndex(index, 1);
            MemoryAccess.setByteAtOffset(segment, index, value);
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
                dst[dstOff + i] = MemoryAccess.getByteAtOffset(segment, index + i);
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
                MemoryAccess.setByteAtOffset(segment, index + i, src[srcOff + i]);
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
            scope.close();
            owner.onFree(capacity);
        }

        private void ensureOpen() {
            if (closed) {
                throw new IllegalStateException("buffer is closed");
            }
            if (!scope.isAlive()) {
                throw new IllegalStateException("scope is not alive");
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
            return MemoryAccess.getByteAtOffset(owner.segment(), offset + index);
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
                dst[dstOff + i] = MemoryAccess.getByteAtOffset(owner.segment(), offset + index + i);
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
