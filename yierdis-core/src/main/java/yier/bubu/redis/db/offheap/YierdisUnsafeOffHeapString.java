package yier.bubu.redis.db.offheap;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.DirectBytesSink;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAddressAllocator;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapSlice;

/**
 * A minimal SDS-like off-heap byte buffer: (ptr,len,cap) semantics with an off-heap header.
 * <p>
 * Memory layout (little-endian):
 * <ul>
 *   <li>{@code int len}</li>
 *   <li>{@code int cap}</li>
 *   <li>{@code byte[cap] data}</li>
 * </ul>
 */
public final class YierdisUnsafeOffHeapString implements AutoCloseable {
    private static final int HEADER_BYTES = Integer.BYTES + Integer.BYTES;

    private final YierdisOffHeapAddressAllocator allocator;
    private long baseAddress;
    private boolean closed;

    public YierdisUnsafeOffHeapString(YierdisOffHeapAddressAllocator allocator, int capacity) {
        if (allocator == null) {
            throw new IllegalArgumentException("allocator must not be null");
        }
        if (capacity < 0) {
            throw new IllegalArgumentException("capacity must be >= 0");
        }
        this.allocator = allocator;

        int total = totalBytesForCapacity(capacity);
        long addr = allocator.allocateAddress(Math.max(8, total));
        this.baseAddress = addr;
        writeInt(addr, 0);
        writeInt(addr + Integer.BYTES, capacity);
    }

    public static YierdisUnsafeOffHeapString fromBytes(YierdisOffHeapAddressAllocator allocator, byte[] src, int off, int len) {
        if (src == null) {
            throw new IllegalArgumentException("src must not be null");
        }
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (off < 0 || off + len > src.length) {
            throw new IndexOutOfBoundsException();
        }

        YierdisUnsafeOffHeapString out = new YierdisUnsafeOffHeapString(allocator, len);
        if (len > 0) {
            allocator.copyMemory(src, off, out.dataAddress(), len);
        }
        out.setLength(len);
        return out;
    }

    public int length() {
        ensureOpen();
        return readInt(baseAddress);
    }

    public int capacity() {
        ensureOpen();
        return readInt(baseAddress + Integer.BYTES);
    }

    public void clear() {
        ensureOpen();
        setLength(0);
    }

    public void overwrite(byte[] src, int off, int len) {
        ensureOpen();
        if (src == null) {
            throw new IllegalArgumentException("src must not be null");
        }
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (off < 0 || off + len > src.length) {
            throw new IndexOutOfBoundsException();
        }
        ensureCapacity(len);
        if (len > 0) {
            allocator.copyMemory(src, off, dataAddress(), len);
        }
        setLength(len);
    }

    public int append(byte[] suffix) {
        ensureOpen();
        if (suffix == null || suffix.length == 0) {
            return length();
        }
        int oldLen = length();
        int newLen = oldLen + suffix.length;
        ensureCapacity(newLen);
        allocator.copyMemory(suffix, 0, dataAddress() + oldLen, suffix.length);
        setLength(newLen);
        return newLen;
    }

    public void ensureCapacity(int requiredLen) {
        ensureOpen();
        if (requiredLen < 0) {
            throw new IllegalArgumentException("requiredLen must be >= 0");
        }
        int cap = capacity();
        if (cap >= requiredLen) {
            return;
        }
        int nextCap = nextCapacity(cap, requiredLen);
        resizeTo(nextCap, length());
    }

    public YierdisOffHeapSlice slice(int index, int len) {
        ensureOpen();
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        int l = length();
        if (index < 0 || index + len > l) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return new YierdisUnsafeOffHeapSlice(this, 0, 0);
        }
        return new YierdisUnsafeOffHeapSlice(this, index, len);
    }

    public YierdisOffHeapSlice slice() {
        ensureOpen();
        int len = length();
        if (len == 0) {
            return new YierdisUnsafeOffHeapSlice(this, 0, 0);
        }
        return new YierdisUnsafeOffHeapSlice(this, 0, len);
    }

    public void getBytes(int index, byte[] dst, int dstOff, int len) {
        ensureOpen();
        if (dst == null) {
            throw new IllegalArgumentException("dst must not be null");
        }
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (dstOff < 0 || dstOff + len > dst.length) {
            throw new IndexOutOfBoundsException();
        }
        int l = length();
        if (index < 0 || index + len > l) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return;
        }
        allocator.copyMemory(dataAddress() + index, dst, dstOff, len);
    }

    public byte getByte(int index) {
        ensureOpen();
        int l = length();
        if (index < 0 || index >= l) {
            throw new IndexOutOfBoundsException();
        }
        return allocator.getByte(dataAddress() + index);
    }

    public void setByte(int index, byte value) {
        ensureOpen();
        int cap = capacity();
        if (index < 0 || index >= cap) {
            throw new IndexOutOfBoundsException();
        }
        allocator.putByte(dataAddress() + index, value);
    }

    public void setBytes(int index, byte[] src, int srcOff, int len) {
        ensureOpen();
        if (src == null) {
            throw new IllegalArgumentException("src must not be null");
        }
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (srcOff < 0 || srcOff + len > src.length) {
            throw new IndexOutOfBoundsException();
        }
        int cap = capacity();
        if (index < 0 || index + len > cap) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return;
        }
        allocator.copyMemory(src, srcOff, dataAddress() + index, len);
    }

    public long dataAddress() {
        ensureOpen();
        return baseAddress + HEADER_BYTES;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        long addr = baseAddress;
        baseAddress = 0;
        int cap = readInt(addr + Integer.BYTES);
        allocator.freeAddress(addr, Math.max(8, totalBytesForCapacity(cap)));
    }

    private void resizeTo(int newCapacity, int copyLen) {
        if (newCapacity < 0) {
            throw new IllegalArgumentException("newCapacity must be >= 0");
        }
        if (copyLen < 0) {
            throw new IllegalArgumentException("copyLen must be >= 0");
        }
        if (copyLen > newCapacity) {
            throw new IllegalArgumentException("copyLen must be <= newCapacity");
        }

        long oldBase = baseAddress;
        int oldCap = readInt(oldBase + Integer.BYTES);
        if (newCapacity == oldCap) {
            return;
        }

        long newBase = allocator.allocateAddress(Math.max(8, totalBytesForCapacity(newCapacity)));
        try {
            writeInt(newBase, copyLen);
            writeInt(newBase + Integer.BYTES, newCapacity);
            if (copyLen > 0) {
                allocator.copyMemory(oldBase + HEADER_BYTES, newBase + HEADER_BYTES, copyLen);
            }
        } catch (RuntimeException e) {
            allocator.freeAddress(newBase, Math.max(8, totalBytesForCapacity(newCapacity)));
            throw e;
        }

        baseAddress = newBase;
        allocator.freeAddress(oldBase, Math.max(8, totalBytesForCapacity(oldCap)));
    }

    public void setLength(int len) {
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        writeInt(baseAddress, len);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("off-heap string is closed");
        }
    }

    private static int totalBytesForCapacity(int cap) {
        return HEADER_BYTES + Math.max(0, cap);
    }

    private static int nextCapacity(int current, int required) {
        int cap = Math.max(16, current);
        while (cap < required) {
            int next = cap < 1024 * 1024 ? (cap << 1) : (cap + 1024 * 1024);
            if (next <= cap) {
                return required;
            }
            cap = next;
        }
        return cap;
    }

    private int readInt(long addr) {
        int b0 = allocator.getByte(addr) & 0xff;
        int b1 = allocator.getByte(addr + 1) & 0xff;
        int b2 = allocator.getByte(addr + 2) & 0xff;
        int b3 = allocator.getByte(addr + 3) & 0xff;
        return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
    }

    private void writeInt(long addr, int value) {
        allocator.putByte(addr, (byte) value);
        allocator.putByte(addr + 1, (byte) (value >>> 8));
        allocator.putByte(addr + 2, (byte) (value >>> 16));
        allocator.putByte(addr + 3, (byte) (value >>> 24));
    }

    private static final class YierdisUnsafeOffHeapSlice implements YierdisOffHeapSlice {
        private static final int COPY_CHUNK_BYTES = 8 * 1024;
        private static final ThreadLocal<byte[]> TL_COPY_BUF =
                ThreadLocal.withInitial(() -> new byte[COPY_CHUNK_BYTES]);

        private final YierdisUnsafeOffHeapString owner;
        private final int offset;
        private final int len;

        private YierdisUnsafeOffHeapSlice(YierdisUnsafeOffHeapString owner, int offset, int len) {
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
            return owner.allocator.getByte(owner.dataAddress() + offset + index);
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
            owner.allocator.copyMemory(owner.dataAddress() + offset + index, dst, dstOff, readLen);
        }

        @Override
        public void writeTo(BytesSink out) {
            owner.ensureOpen();
            if (out == null) {
                throw new IllegalArgumentException("out must not be null");
            }
            if (len == 0) {
                return;
            }

            long srcAddr = owner.dataAddress() + offset;
            if (out instanceof DirectBytesSink directSink && directSink.hasMemoryAddress()) {
                int before = directSink.writerIndex();
                directSink.ensureWritable(len);
                long dstAddr = directSink.memoryAddress() + before;
                owner.allocator.copyMemory(srcAddr, dstAddr, len);
                directSink.writerIndex(before + len);
                return;
            }

            byte[] scratch = TL_COPY_BUF.get();
            int remaining = len;
            long addr = srcAddr;
            while (remaining > 0) {
                int chunk = Math.min(remaining, scratch.length);
                owner.allocator.copyMemory(addr, scratch, 0, chunk);
                out.writeBytes(scratch, 0, chunk);
                addr += chunk;
                remaining -= chunk;
            }
        }
    }
}
