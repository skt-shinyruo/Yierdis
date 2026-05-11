package yier.bubu.redis.memory.foreign;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSource;
import yier.bubu.redis.memory.api.OffHeapAllocator;
import yier.bubu.redis.memory.api.OffHeapBuf;
import yier.bubu.redis.memory.api.OffHeapOutOfMemoryException;
import yier.bubu.redis.memory.api.OffHeapSlice;

public final class YierdisForeignOffHeapAllocator implements OffHeapAllocator {
    private final long maxBytes;
    private final YierdisFfmMemoryRuntime runtime;
    private final boolean ownsRuntime;
    private YierdisFfmSlabAllocator slabAllocator;

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
        this.slabAllocator = newSlabAllocator();
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

        OffHeapBuf buf = new YierdisForeignOffHeapBuf(this, slabAllocator.allocate(capacity), capacity);
        usedBytes = next;
        return buf;
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
        slabAllocator.close();
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
        releaseIdleSlabs();
    }

    private YierdisFfmSlabAllocator newSlabAllocator() {
        return new YierdisFfmSlabAllocator(runtime, maxBytes);
    }

    private void releaseIdleSlabs() {
        if (closed || usedBytes != 0) {
            return;
        }
        slabAllocator.close();
        slabAllocator = newSlabAllocator();
    }

    private static final class YierdisForeignOffHeapBuf implements OffHeapBuf {
        private final YierdisForeignOffHeapAllocator owner;
        private final OffHeapBuf delegate;
        private final int capacity;

        private boolean closed;

        private YierdisForeignOffHeapBuf(
                YierdisForeignOffHeapAllocator owner,
                OffHeapBuf delegate,
                int capacity
        ) {
            this.owner = owner;
            this.delegate = delegate;
            this.capacity = capacity;
        }

        @Override
        public int capacity() {
            return capacity;
        }

        @Override
        public byte getByte(int index) {
            return delegate.getByte(index);
        }

        @Override
        public void setByte(int index, byte value) {
            delegate.setByte(index, value);
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            delegate.getBytes(index, dst, dstOff, len);
        }

        @Override
        public void setBytes(int index, byte[] src, int srcOff, int len) {
            delegate.setBytes(index, src, srcOff, len);
        }

        @Override
        public void setBytes(int index, BytesSource src, int srcIndex, int len) {
            delegate.setBytes(index, src, srcIndex, len);
        }

        @Override
        public OffHeapSlice slice(int index, int len) {
            return delegate.slice(index, len);
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            delegate.close();
            owner.onFree(capacity);
        }
    }
}
