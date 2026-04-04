package yier.bubu.redis.db.memory.foreign;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSource;
import yier.bubu.redis.offheap.api.OffHeapAllocator;
import yier.bubu.redis.offheap.api.OffHeapBuf;
import yier.bubu.redis.offheap.api.OffHeapOutOfMemoryException;
import yier.bubu.redis.offheap.api.OffHeapSlice;

import java.nio.ByteBuffer;

public final class YierdisForeignOffHeapAllocator implements OffHeapAllocator {
    private final long maxBytes;
    private final YierdisFfmMemoryRuntime runtime;
    private final boolean ownsRuntime;

    private boolean closed;
    private long usedBytes;

    public YierdisForeignOffHeapAllocator(long maxBytes) {
        this(new YierdisFfmMemoryRuntime("foreign-allocator"), maxBytes, true);
    }

    public YierdisForeignOffHeapAllocator(YierdisFfmMemoryRuntime runtime, long maxBytes) {
        this(runtime, maxBytes, false);
    }

    private YierdisForeignOffHeapAllocator(YierdisFfmMemoryRuntime runtime, long maxBytes, boolean ownsRuntime) {
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must be >= 0");
        }
        this.runtime = runtime;
        this.maxBytes = maxBytes;
        this.ownsRuntime = ownsRuntime;
    }

    @Override
    public OffHeapBuf allocate(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        if (closed) {
            throw new IllegalStateException("allocator is closed");
        }

        long next = usedBytes + capacity;
        if (maxBytes > 0 && next > maxBytes) {
            throw new OffHeapOutOfMemoryException("off-heap memory limit exceeded");
        }

        YierdisFfmRegion region = runtime.allocateRegion("buf", capacity);
        usedBytes = next;
        return new YierdisForeignOffHeapBuf(this, region, capacity);
    }

    @Override
    public long usedBytes() {
        return usedBytes;
    }

    @Override
    public long maxBytes() {
        return maxBytes;
    }

    public YierdisFfmMemoryRuntime memoryRuntime() {
        return runtime;
    }

    @Override
    public void close() {
        closed = true;
        if (usedBytes != 0) {
            throw new IllegalStateException("off-heap leak: " + usedBytes + " bytes still allocated");
        }
        if (ownsRuntime) {
            runtime.close();
        }
    }

    void onFree(int capacity) {
        long next = usedBytes - capacity;
        if (next < 0) {
            throw new IllegalStateException("allocator accounting underflow");
        }
        usedBytes = next;
    }

    private static final class YierdisForeignOffHeapBuf implements OffHeapBuf {
        private static final int COPY_CHUNK_BYTES = 8 * 1024;
        private static final ThreadLocal<byte[]> TL_COPY_BUF =
                ThreadLocal.withInitial(() -> new byte[COPY_CHUNK_BYTES]);

        private final YierdisForeignOffHeapAllocator owner;
        private final YierdisFfmRegion region;
        private final YierdisFfmSpan span;
        private final int capacity;

        private boolean closed;

        private YierdisForeignOffHeapBuf(
                YierdisForeignOffHeapAllocator owner,
                YierdisFfmRegion region,
                int capacity
        ) {
            this.owner = owner;
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
        public OffHeapSlice slice(int index, int len) {
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
            owner.onFree(capacity);
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

    private static final class YierdisForeignOffHeapSlice implements OffHeapSlice {
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
