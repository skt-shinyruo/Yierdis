package yier.bubu.redis.db.offheap.unsafe;

import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBackend;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBuf;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapOutOfMemoryException;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapSlice;

/**
 * Unsafe-based off-heap allocator backed by Netty's {@link PlatformDependent} utilities.
 * <p>
 * This backend avoids Java Foreign Memory API incubator modules while still providing:
 * - deterministic free ({@link YierdisOffHeapBuf#close()})
 * - accounting + optional max-bytes limit
 * - slice views that can be written to Netty {@link ByteBuf} without heap copies
 */
public final class YierdisUnsafeOffHeapAllocator implements YierdisOffHeapAllocator {
    private final long maxBytes;

    private boolean closed;
    private long usedBytes;

    public YierdisUnsafeOffHeapAllocator(long maxBytes) {
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must be >= 0");
        }
        this.maxBytes = maxBytes;
    }

    /**
     * Allocates a raw off-heap block and returns its address. The returned block MUST be closed exactly once.
     * <p>
     * This is intentionally unsafe and backend-specific; it is meant for internal off-heap index structures
     * that need address-based access (e.g. hash tables, slot arrays).
     */
    public YierdisUnsafeOffHeapBlock allocateBlock(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        long address = allocateAddressInternal(capacity);
        return new YierdisUnsafeOffHeapBlock(this, address, capacity);
    }

    public long allocateAddress(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        return allocateAddressInternal(capacity);
    }

    public void freeAddress(long address, int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        if (address == 0) {
            throw new IllegalArgumentException("address must be != 0");
        }
        PlatformDependent.freeMemory(address);
        onFree(capacity);
    }

    @Override
    public YierdisOffHeapBuf allocate(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        long address = allocateAddressInternal(capacity);
        return new YierdisUnsafeOffHeapBuf(this, address, capacity);
    }

    private long allocateAddressInternal(int capacity) {
        if (closed) {
            throw new IllegalStateException("allocator is closed");
        }

        long next = usedBytes + capacity;
        if (maxBytes > 0 && next > maxBytes) {
            throw new YierdisOffHeapOutOfMemoryException("off-heap memory limit exceeded");
        }

        long address = PlatformDependent.allocateMemory(capacity);
        usedBytes = next;
        return address;
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
        return YierdisOffHeapBackend.UNSAFE;
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

    public static final class YierdisUnsafeOffHeapBlock implements AutoCloseable {
        private final YierdisUnsafeOffHeapAllocator owner;
        private final int capacity;
        private long address;
        private boolean closed;

        YierdisUnsafeOffHeapBlock(YierdisUnsafeOffHeapAllocator owner, long address, int capacity) {
            this.owner = owner;
            this.address = address;
            this.capacity = capacity;
        }

        public long address() {
            return address;
        }

        public int capacity() {
            return capacity;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            long addr = address;
            address = 0;
            owner.freeAddress(addr, capacity);
        }
    }
}

final class YierdisUnsafeOffHeapBuf implements YierdisOffHeapBuf {
    private final YierdisUnsafeOffHeapAllocator owner;
    private final int capacity;
    private long address;

    private boolean closed;

    YierdisUnsafeOffHeapBuf(YierdisUnsafeOffHeapAllocator owner, long address, int capacity) {
        this.owner = owner;
        this.address = address;
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
        return PlatformDependent.getByte(address + index);
    }

    @Override
    public void setByte(int index, byte value) {
        ensureOpen();
        checkIndex(index, 1);
        PlatformDependent.putByte(address + index, value);
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
        if (len == 0) {
            return;
        }
        PlatformDependent.copyMemory(address + index, dst, dstOff, len);
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
        if (len == 0) {
            return;
        }
        PlatformDependent.copyMemory(src, srcOff, address + index, len);
    }

    @Override
    public YierdisOffHeapSlice slice(int index, int len) {
        ensureOpen();
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        checkIndex(index, len);
        return new YierdisUnsafeOffHeapSlice(this, index, len);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        long addr = address;
        address = 0;
        PlatformDependent.freeMemory(addr);
        owner.onFree(capacity);
    }

    void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("buffer is closed");
        }
    }

    private void checkIndex(int index, int len) {
        if (index < 0 || index + len > capacity) {
            throw new IndexOutOfBoundsException();
        }
    }

    long address() {
        return address;
    }
}

final class YierdisUnsafeOffHeapSlice implements YierdisOffHeapSlice {
    private static final int COPY_CHUNK_BYTES = 8 * 1024;
    private static final ThreadLocal<byte[]> TL_COPY_BUF =
            ThreadLocal.withInitial(() -> new byte[COPY_CHUNK_BYTES]);

    private final YierdisUnsafeOffHeapBuf owner;
    private final int offset;
    private final int len;

    YierdisUnsafeOffHeapSlice(YierdisUnsafeOffHeapBuf owner, int offset, int len) {
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
        owner.ensureOpen();
        if (index < 0 || index >= len) {
            throw new IndexOutOfBoundsException();
        }
        return PlatformDependent.getByte(owner.address() + offset + index);
    }

    @Override
    public void getBytes(int index, byte[] dst, int dstOff, int readLen) {
        owner.ensureOpen();
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
        if (readLen == 0) {
            return;
        }
        PlatformDependent.copyMemory(owner.address() + offset + index, dst, dstOff, readLen);
    }

    @Override
    public void writeTo(ByteBuf out) {
        owner.ensureOpen();
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }
        if (len == 0) {
            return;
        }

        long srcAddr = owner.address() + offset;
        int before = out.writerIndex();
        out.ensureWritable(len);

        if (out.hasMemoryAddress()) {
            long dstAddr = out.memoryAddress() + before;
            PlatformDependent.copyMemory(srcAddr, dstAddr, len);
            out.writerIndex(before + len);
            return;
        }

        // Fallback for heap ByteBuf implementations: copy via a reusable heap buffer.
        byte[] scratch = TL_COPY_BUF.get();
        int remaining = len;
        long addr = srcAddr;
        while (remaining > 0) {
            int chunk = Math.min(remaining, scratch.length);
            PlatformDependent.copyMemory(addr, scratch, 0, chunk);
            out.writeBytes(scratch, 0, chunk);
            addr += chunk;
            remaining -= chunk;
        }
    }
}
