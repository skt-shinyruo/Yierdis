package yier.bubu.redis.db.offheap.unsafe;

import io.netty.util.internal.PlatformDependent;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAddressAllocator;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBackend;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBlock;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBuf;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapOutOfMemoryException;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapSlice;
import yier.bubu.redis.bytes.BytesSource;
import yier.bubu.redis.bytes.DirectBytesSink;

/**
 * Unsafe-based off-heap allocator backed by Netty's {@link PlatformDependent} utilities.
 * <p>
 * This backend avoids Java Foreign Memory API incubator modules while still providing:
 * - deterministic free ({@link YierdisOffHeapBuf#close()})
 * - accounting + optional max-bytes limit
 * - slice views that can be written to Netty {@link ByteBuf} without heap copies
 */
public final class YierdisUnsafeOffHeapAllocator implements YierdisOffHeapAddressAllocator {
    private static final int INTERNAL_HEADER_BYTES = Long.BYTES;
    private static final int INTERNAL_HEADER_MAGIC = 0x59494552; // "YIER"
    private static final int MIN_BLOCK_BYTES = 8;
    private static final int MAX_SMALL_BLOCK_BYTES = 64 * 1024;
    private static final int MIN_BLOCK_SHIFT = 3; // 2^3 == 8
    private static final int MAX_SMALL_BLOCK_SHIFT = 16; // 2^16 == 65536
    private static final int SIZE_CLASSES = MAX_SMALL_BLOCK_SHIFT - MIN_BLOCK_SHIFT + 1;

    private final long maxBytes;
    private final long[] freeListHeads = new long[SIZE_CLASSES];
    private final int[] freeListSizes = new int[SIZE_CLASSES];

    private final int[] liveAllocCounts = new int[SIZE_CLASSES];
    private final long[] liveAllocBytes = new long[SIZE_CLASSES];
    private int liveLargeAllocs;
    private long liveLargeBytes;

    private boolean closed;
    private long usedBytes;
    private long reservedBytes;

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
    @Override
    public YierdisOffHeapBlock allocateBlock(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        long address = allocateAddressInternal(capacity);
        return new YierdisUnsafeOffHeapBlock(this, address, capacity);
    }

    @Override
    public long allocateAddress(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        return allocateAddressInternal(capacity);
    }

    @Override
    public void freeAddress(long address, int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        if (address == 0) {
            throw new IllegalArgumentException("address must be != 0");
        }

        long baseAddress = address - INTERNAL_HEADER_BYTES;
        if (baseAddress <= 0) {
            throw new IllegalArgumentException("address must be > " + INTERNAL_HEADER_BYTES);
        }

        int allocBytes = decodeAllocBytes(baseAddress);
        onFreeLive(allocBytes);
        if (closed || allocBytes > MAX_SMALL_BLOCK_BYTES) {
            PlatformDependent.freeMemory(baseAddress);
            onFree(allocBytes);
            reservedBytes -= allocBytes;
            if (reservedBytes < 0) {
                throw new IllegalStateException("allocator reservedBytes underflow");
            }
            return;
        }

        int classIndex = classIndexForAllocationBytes(allocBytes);
        long head = freeListHeads[classIndex];
        writeLong(baseAddress, head);
        freeListHeads[classIndex] = baseAddress;
        freeListSizes[classIndex]++;
        onFree(allocBytes);
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

        int allocBytes = allocationBytesFor(capacity);
        if (allocBytes <= MAX_SMALL_BLOCK_BYTES) {
            int classIndex = classIndexForAllocationBytes(allocBytes);
            long head = freeListHeads[classIndex];
            if (head != 0) {
                long next = readLong(head);
                freeListHeads[classIndex] = next;
                freeListSizes[classIndex]--;
                usedBytes += allocBytes;
                onAllocLive(allocBytes);
                writeLong(head, encodeHeader(allocBytes));
                return head + INTERNAL_HEADER_BYTES;
            }
        }

        ensureCanReserve(allocBytes);
        long address = PlatformDependent.allocateMemory(allocBytes + INTERNAL_HEADER_BYTES);
        reservedBytes += allocBytes;
        usedBytes += allocBytes;
        onAllocLive(allocBytes);
        writeLong(address, encodeHeader(allocBytes));
        return address + INTERNAL_HEADER_BYTES;
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
            // Best-effort: free anything already returned to our free lists so the process doesn't leak
            // too much native memory on assertion failures.
            freeAllFreeLists();
            throw new IllegalStateException("off-heap leak: " + usedBytes + " bytes still allocated (" + liveSummary() + ")");
        }
        freeAllFreeLists();
        if (reservedBytes != 0) {
            throw new IllegalStateException("off-heap reserved bytes leak: " + reservedBytes + " bytes still reserved");
        }
    }

    @Override
    public byte getByte(long address) {
        if (address == 0) {
            throw new IllegalArgumentException("address must be != 0");
        }
        return YierdisUnsafeAccess.getByte(address);
    }

    @Override
    public void putByte(long address, byte value) {
        if (address == 0) {
            throw new IllegalArgumentException("address must be != 0");
        }
        YierdisUnsafeAccess.putByte(address, value);
    }

    @Override
    public void setMemory(long address, long bytes, byte value) {
        if (address == 0) {
            throw new IllegalArgumentException("address must be != 0");
        }
        YierdisUnsafeAccess.setMemory(address, bytes, value);
    }

    @Override
    public void copyMemory(long srcAddress, long dstAddress, long bytes) {
        if (srcAddress == 0 || dstAddress == 0) {
            throw new IllegalArgumentException("address must be != 0");
        }
        YierdisUnsafeAccess.copyMemory(srcAddress, dstAddress, bytes);
    }

    @Override
    public void copyMemory(byte[] src, int srcIndex, long dstAddress, int len) {
        if (dstAddress == 0) {
            throw new IllegalArgumentException("dstAddress must be != 0");
        }
        YierdisUnsafeAccess.copyMemory(src, srcIndex, dstAddress, len);
    }

    @Override
    public void copyMemory(long srcAddress, byte[] dst, int dstIndex, int len) {
        if (srcAddress == 0) {
            throw new IllegalArgumentException("srcAddress must be != 0");
        }
        YierdisUnsafeAccess.copyMemory(srcAddress, dst, dstIndex, len);
    }

    void onFree(int allocBytes) {
        long next = usedBytes - allocBytes;
        if (next < 0) {
            throw new IllegalStateException("allocator accounting underflow");
        }
        usedBytes = next;
    }

    private void ensureCanReserve(int allocBytes) {
        if (maxBytes <= 0) {
            return;
        }
        if (allocBytes > maxBytes) {
            throw new YierdisOffHeapOutOfMemoryException("off-heap memory limit exceeded");
        }
        long next = reservedBytes + allocBytes;
        if (next <= maxBytes) {
            return;
        }
        trimFreeListsFor(next - maxBytes);
        if (reservedBytes + allocBytes > maxBytes) {
            throw new YierdisOffHeapOutOfMemoryException("off-heap memory limit exceeded");
        }
    }

    private void trimFreeListsFor(long bytesToFree) {
        long remaining = bytesToFree;
        for (int classIndex = SIZE_CLASSES - 1; classIndex >= 0 && remaining > 0; classIndex--) {
            int allocBytes = allocationBytesForClass(classIndex);
            while (freeListHeads[classIndex] != 0 && remaining > 0) {
                long head = freeListHeads[classIndex];
                long next = readLong(head);
                freeListHeads[classIndex] = next;
                freeListSizes[classIndex]--;
                PlatformDependent.freeMemory(head);
                reservedBytes -= allocBytes;
                remaining -= allocBytes;
            }
        }
        if (reservedBytes < 0) {
            throw new IllegalStateException("allocator reservedBytes underflow");
        }
    }

    private void freeAllFreeLists() {
        for (int classIndex = 0; classIndex < SIZE_CLASSES; classIndex++) {
            int allocBytes = allocationBytesForClass(classIndex);
            long head = freeListHeads[classIndex];
            while (head != 0) {
                long next = readLong(head);
                PlatformDependent.freeMemory(head);
                reservedBytes -= allocBytes;
                head = next;
            }
            freeListHeads[classIndex] = 0L;
            freeListSizes[classIndex] = 0;
        }
        if (reservedBytes < 0) {
            throw new IllegalStateException("allocator reservedBytes underflow");
        }
    }

    private static int allocationBytesFor(int capacity) {
        int aligned = align8(capacity);
        if (aligned <= MAX_SMALL_BLOCK_BYTES) {
            int nextPow2 = 1 << (32 - Integer.numberOfLeadingZeros(Math.max(MIN_BLOCK_BYTES, aligned) - 1));
            return Math.min(MAX_SMALL_BLOCK_BYTES, nextPow2);
        }
        return aligned;
    }

    private static long encodeHeader(int allocBytes) {
        return ((long) INTERNAL_HEADER_MAGIC << 32) | (allocBytes & 0xffffffffL);
    }

    private static int decodeAllocBytes(long baseAddress) {
        long header = readLong(baseAddress);
        int magic = (int) (header >>> 32);
        if (magic != INTERNAL_HEADER_MAGIC) {
            throw new IllegalStateException("invalid off-heap address or double-free detected");
        }
        return (int) header;
    }

    private void onAllocLive(int allocBytes) {
        if (allocBytes <= MAX_SMALL_BLOCK_BYTES) {
            int classIndex = classIndexForAllocationBytes(allocBytes);
            liveAllocCounts[classIndex]++;
            liveAllocBytes[classIndex] += allocBytes;
            return;
        }
        liveLargeAllocs++;
        liveLargeBytes += allocBytes;
    }

    private void onFreeLive(int allocBytes) {
        if (allocBytes <= MAX_SMALL_BLOCK_BYTES) {
            int classIndex = classIndexForAllocationBytes(allocBytes);
            liveAllocCounts[classIndex]--;
            liveAllocBytes[classIndex] -= allocBytes;
            return;
        }
        liveLargeAllocs--;
        liveLargeBytes -= allocBytes;
    }

    private String liveSummary() {
        StringBuilder sb = new StringBuilder(64);
        boolean first = true;
        for (int classIndex = 0; classIndex < SIZE_CLASSES; classIndex++) {
            int count = liveAllocCounts[classIndex];
            if (count <= 0) {
                continue;
            }
            int bytes = allocationBytesForClass(classIndex);
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(bytes).append("B x").append(count);
        }
        if (liveLargeAllocs > 0) {
            if (!first) {
                sb.append(", ");
            }
            sb.append("large ").append(liveLargeBytes).append("B x").append(liveLargeAllocs);
            first = false;
        }
        if (first) {
            return "no live allocation detail";
        }
        return sb.toString();
    }

    private static int align8(int v) {
        int x = v + 7;
        return x & ~7;
    }

    private static int allocationBytesForClass(int classIndex) {
        return 1 << (MIN_BLOCK_SHIFT + classIndex);
    }

    private static int classIndexForAllocationBytes(int allocBytes) {
        if (allocBytes < MIN_BLOCK_BYTES || (allocBytes & (allocBytes - 1)) != 0) {
            throw new IllegalArgumentException("allocBytes must be a power-of-two >= " + MIN_BLOCK_BYTES);
        }
        int shift = 31 - Integer.numberOfLeadingZeros(allocBytes);
        int index = shift - MIN_BLOCK_SHIFT;
        if (index < 0 || index >= SIZE_CLASSES) {
            throw new IllegalArgumentException("allocBytes is not a small-block size class: " + allocBytes);
        }
        return index;
    }

    private static long readLong(long addr) {
        long b0 = PlatformDependent.getByte(addr) & 0xffL;
        long b1 = PlatformDependent.getByte(addr + 1) & 0xffL;
        long b2 = PlatformDependent.getByte(addr + 2) & 0xffL;
        long b3 = PlatformDependent.getByte(addr + 3) & 0xffL;
        long b4 = PlatformDependent.getByte(addr + 4) & 0xffL;
        long b5 = PlatformDependent.getByte(addr + 5) & 0xffL;
        long b6 = PlatformDependent.getByte(addr + 6) & 0xffL;
        long b7 = PlatformDependent.getByte(addr + 7) & 0xffL;
        return b0
                | (b1 << 8)
                | (b2 << 16)
                | (b3 << 24)
                | (b4 << 32)
                | (b5 << 40)
                | (b6 << 48)
                | (b7 << 56);
    }

    private static void writeLong(long addr, long value) {
        PlatformDependent.putByte(addr, (byte) value);
        PlatformDependent.putByte(addr + 1, (byte) (value >>> 8));
        PlatformDependent.putByte(addr + 2, (byte) (value >>> 16));
        PlatformDependent.putByte(addr + 3, (byte) (value >>> 24));
        PlatformDependent.putByte(addr + 4, (byte) (value >>> 32));
        PlatformDependent.putByte(addr + 5, (byte) (value >>> 40));
        PlatformDependent.putByte(addr + 6, (byte) (value >>> 48));
        PlatformDependent.putByte(addr + 7, (byte) (value >>> 56));
    }

    public static final class YierdisUnsafeOffHeapBlock implements YierdisOffHeapBlock {
        private final YierdisUnsafeOffHeapAllocator owner;
        private final int capacity;
        private long address;
        private boolean closed;

        YierdisUnsafeOffHeapBlock(YierdisUnsafeOffHeapAllocator owner, long address, int capacity) {
            this.owner = owner;
            this.address = address;
            this.capacity = capacity;
        }

        @Override
        public long address() {
            return address;
        }

        @Override
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
    private static final int COPY_CHUNK_BYTES = 8 * 1024;
    private static final ThreadLocal<byte[]> TL_COPY_BUF =
            ThreadLocal.withInitial(() -> new byte[COPY_CHUNK_BYTES]);

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

        long dst = address + index;
        if (src.hasMemoryAddress()) {
            PlatformDependent.copyMemory(src.memoryAddress() + srcIndex, dst, len);
            return;
        }

        byte[] scratch = TL_COPY_BUF.get();
        int remaining = len;
        int off = 0;
        while (remaining > 0) {
            int chunk = Math.min(remaining, scratch.length);
            src.getBytes(srcIndex + off, scratch, 0, chunk);
            PlatformDependent.copyMemory(scratch, 0, dst + off, chunk);
            off += chunk;
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
        owner.freeAddress(addr, capacity);
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
    public void writeTo(BytesSink out) {
        owner.ensureOpen();
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }
        if (len == 0) {
            return;
        }

        long srcAddr = owner.address() + offset;
        if (out instanceof DirectBytesSink directSink && directSink.hasMemoryAddress()) {
            int before = directSink.writerIndex();
            directSink.ensureWritable(len);
            long dstAddr = directSink.memoryAddress() + before;
            PlatformDependent.copyMemory(srcAddr, dstAddr, len);
            directSink.writerIndex(before + len);
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
