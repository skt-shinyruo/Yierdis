package yier.bubu.redis.db.memory.foreign;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSource;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBackend;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBuf;
import yier.bubu.redis.db.memory.api.YierdisOffHeapOutOfMemoryException;
import yier.bubu.redis.db.memory.api.YierdisOffHeapSlice;
import yier.bubu.redis.offheap.api.OffHeapAllocator;

import java.nio.ByteBuffer;

public final class YierdisForeignOffHeapAllocator implements OffHeapAllocator {
    private final long maxBytes;
    private final YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("foreign-allocator");

    private boolean closed;

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

        long next = runtime.usedBytes() + capacity;
        if (maxBytes > 0 && next > maxBytes) {
            throw new YierdisOffHeapOutOfMemoryException("off-heap memory limit exceeded");
        }

        YierdisFfmRegion region = runtime.allocateRegion("buf", capacity);
        return new YierdisForeignOffHeapBuf(region, capacity);
    }

    @Override
    public long usedBytes() {
        return runtime.usedBytes();
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
        runtime.close();
    }

    private static final class YierdisForeignOffHeapBuf implements YierdisOffHeapBuf {
        private static final int COPY_CHUNK_BYTES = 8 * 1024;
        private static final ThreadLocal<byte[]> TL_COPY_BUF =
                ThreadLocal.withInitial(() -> new byte[COPY_CHUNK_BYTES]);

        private final YierdisFfmRegion region;
        private final YierdisFfmSpan span;
        private final int capacity;

        private boolean closed;

        private YierdisForeignOffHeapBuf(YierdisFfmRegion region, int capacity) {
            this.region = region;
            this.span = region.span(0, capacity);
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
            return YierdisFfmAccess.getByte(span, index);
        }

        @Override
        public void setByte(int index, byte value) {
            ensureOpen();
            checkIndex(index, 1);
            YierdisFfmAccess.setByte(span, index, value);
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
            YierdisFfmAccess.getBytes(span, index, dst, dstOff, len);
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
            YierdisFfmAccess.setBytes(span, index, src, srcOff, len);
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

            ByteBuffer dst = YierdisFfmAccess.asByteBuffer(span, index, len);
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
            return new YierdisForeignOffHeapSlice(this, span.slice(index, len));
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            region.close();
        }

        private void ensureOpen() {
            if (closed) {
                throw new IllegalStateException("buffer is closed");
            }
            region.ensureOpen();
        }

        private void checkIndex(int index, int len) {
            if (index < 0 || index + len > capacity) {
                throw new IndexOutOfBoundsException();
            }
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
        private final YierdisFfmSpan span;

        private YierdisForeignOffHeapSlice(YierdisForeignOffHeapBuf owner, YierdisFfmSpan span) {
            this.owner = owner;
            this.span = span;
        }

        @Override
        public int length() {
            return span.size();
        }

        @Override
        public byte getByte(int index) {
            owner.ensureOwnerOpen();
            return YierdisFfmAccess.getByte(span, index);
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
            YierdisFfmAccess.getBytes(span, index, dst, dstOff, readLen);
        }

        @Override
        public void writeTo(BytesSink out) {
            owner.ensureOwnerOpen();
            if (out == null) {
                throw new IllegalArgumentException("out must not be null");
            }
            if (span.size() == 0) {
                return;
            }

            ByteBuffer bb = YierdisFfmAccess.asByteBuffer(span);

            byte[] scratch = TL_COPY_BUF.get();
            int remaining = span.size();
            while (remaining > 0) {
                int chunk = Math.min(remaining, scratch.length);
                bb.get(scratch, 0, chunk);
                out.writeBytes(scratch, 0, chunk);
                remaining -= chunk;
            }
        }
    }
}
