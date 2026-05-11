package yier.bubu.redis.memory.foreign;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSource;
import yier.bubu.redis.memory.api.OffHeapAllocator;
import yier.bubu.redis.memory.api.OffHeapBuf;
import yier.bubu.redis.memory.api.OffHeapOutOfMemoryException;
import yier.bubu.redis.memory.api.OffHeapSlice;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Objects;

public final class YierdisFfmSlabAllocator implements OffHeapAllocator {
    private static final int DEFAULT_SLAB_BYTES = 64 * 1024;

    private final YierdisFfmMemoryRuntime runtime;
    private final int defaultSlabBytes;
    private final ArrayList<YierdisFfmSlab> slabs = new ArrayList<>();

    private boolean closed;
    private long usedBytes;

    public YierdisFfmSlabAllocator(YierdisFfmMemoryRuntime runtime) {
        this(runtime, DEFAULT_SLAB_BYTES);
    }

    public YierdisFfmSlabAllocator(YierdisFfmMemoryRuntime runtime, int defaultSlabBytes) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        if (defaultSlabBytes <= 0) {
            throw new IllegalArgumentException("defaultSlabBytes must be > 0");
        }
        this.defaultSlabBytes = defaultSlabBytes;
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
        if (next < 0 || (maxBytes() > 0 && next > maxBytes())) {
            throw new OffHeapOutOfMemoryException("off-heap memory limit exceeded");
        }

        YierdisFfmSlab.Allocation allocation = null;
        YierdisFfmSlab slab = null;
        for (int i = 0; i < slabs.size(); i++) {
            YierdisFfmSlab candidate = slabs.get(i);
            allocation = candidate.allocate(capacity);
            if (allocation != null) {
                slab = candidate;
                break;
            }
        }

        if (allocation == null) {
            slab = new YierdisFfmSlab(runtime, slabBytesForNewSlab(capacity));
            slabs.add(slab);
            allocation = slab.allocate(capacity);
        }

        usedBytes = next;
        return new YierdisSlabBackedOffHeapBuf(this, slab, allocation, capacity);
    }

    @Override
    public long usedBytes() {
        return usedBytes;
    }

    @Override
    public long maxBytes() {
        return 0;
    }

    @Override
    public void close() {
        closed = true;
        if (usedBytes != 0) {
            throw new IllegalStateException("off-heap leak: " + usedBytes + " bytes still allocated");
        }
        for (int i = 0; i < slabs.size(); i++) {
            slabs.get(i).close();
        }
        slabs.clear();
    }

    void onFree(int capacity) {
        if (closed) {
            return;
        }
        long next = usedBytes - capacity;
        if (next < 0) {
            throw new IllegalStateException("allocator accounting underflow");
        }
        usedBytes = next;
    }

    void free(YierdisFfmSlab slab, int offset, int capacity) {
        slab.free(offset, capacity);
    }

    private int slabBytesForNewSlab(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        long remaining = Long.MAX_VALUE;
        if (maxBytes() > 0) {
            remaining = maxBytes() - usedBytes;
            if (remaining < capacity) {
                throw new OffHeapOutOfMemoryException("off-heap memory limit exceeded");
            }
        }

        long preferred = Math.max(capacity, defaultSlabBytes);
        if (maxBytes() > 0) {
            preferred = Math.min(preferred, remaining);
        }
        return Math.toIntExact(preferred);
    }

    private static final class YierdisSlabBackedOffHeapBuf implements OffHeapBuf {
        private static final int COPY_CHUNK_BYTES = 8 * 1024;
        private static final ThreadLocal<byte[]> TL_COPY_BUF =
                ThreadLocal.withInitial(() -> new byte[COPY_CHUNK_BYTES]);

        private final YierdisFfmSlabAllocator owner;
        private final YierdisFfmSlab slab;
        private final YierdisFfmSlab.Allocation allocation;
        private final YierdisFfmSpan span;
        private final int capacity;

        private boolean closed;

        private YierdisSlabBackedOffHeapBuf(
                YierdisFfmSlabAllocator owner,
                YierdisFfmSlab slab,
                YierdisFfmSlab.Allocation allocation,
                int capacity
        ) {
            this.owner = owner;
            this.slab = slab;
            this.allocation = allocation;
            this.span = slab.span(allocation.offset(), capacity);
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
            return new YierdisSlabBackedOffHeapSlice(this, span.slice(index, len));
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            owner.free(slab, allocation.offset(), capacity);
            owner.onFree(capacity);
        }

        private void ensureOpen() {
            if (closed) {
                throw new IllegalStateException("buffer is closed");
            }
            slab.span(allocation.offset(), capacity);
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

    private static final class YierdisSlabBackedOffHeapSlice implements OffHeapSlice {
        private static final int COPY_CHUNK_BYTES = 8 * 1024;
        private static final ThreadLocal<byte[]> TL_COPY_BUF =
                ThreadLocal.withInitial(() -> new byte[COPY_CHUNK_BYTES]);

        private final YierdisSlabBackedOffHeapBuf owner;
        private final YierdisFfmSpan span;

        private YierdisSlabBackedOffHeapSlice(YierdisSlabBackedOffHeapBuf owner, YierdisFfmSpan span) {
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
