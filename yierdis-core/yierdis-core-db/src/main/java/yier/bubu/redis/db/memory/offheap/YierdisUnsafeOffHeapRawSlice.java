package yier.bubu.redis.db.memory.offheap;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.DirectBytesSink;
import yier.bubu.redis.offheap.api.OffHeapAddressAllocator;
import yier.bubu.redis.offheap.api.OffHeapSlice;

/**
 * A raw off-heap slice backed by an absolute address.
 * <p>
 * This slice does not provide ownership/lifecycle management; callers MUST ensure the underlying
 * memory remains valid for the duration of usage (typically within a single command execution).
 */
public final class YierdisUnsafeOffHeapRawSlice implements OffHeapSlice {
    private static final int COPY_CHUNK_BYTES = 8 * 1024;
    private static final ThreadLocal<byte[]> TL_COPY_BUF =
            ThreadLocal.withInitial(() -> new byte[COPY_CHUNK_BYTES]);

    private final OffHeapAddressAllocator allocator;
    private final long address;
    private final int len;

    public YierdisUnsafeOffHeapRawSlice(OffHeapAddressAllocator allocator, long address, int len) {
        if (allocator == null) {
            throw new IllegalArgumentException("allocator must not be null");
        }
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (address == 0 && len > 0) {
            throw new IllegalArgumentException("address must be != 0 when len > 0");
        }
        this.allocator = allocator;
        this.address = address;
        this.len = len;
    }

    @Override
    public int length() {
        return len;
    }

    @Override
    public byte getByte(int index) {
        if (index < 0 || index >= len) {
            throw new IndexOutOfBoundsException();
        }
        return allocator.getByte(address + index);
    }

    @Override
    public void getBytes(int index, byte[] dst, int dstOff, int readLen) {
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
        allocator.copyMemory(address + index, dst, dstOff, readLen);
    }

    @Override
    public void writeTo(BytesSink out) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }
        if (len == 0) {
            return;
        }

        if (out instanceof DirectBytesSink directSink && directSink.hasMemoryAddress()) {
            int before = directSink.writerIndex();
            directSink.ensureWritable(len);
            long dstAddr = directSink.memoryAddress() + before;
            allocator.copyMemory(address, dstAddr, len);
            directSink.writerIndex(before + len);
            return;
        }

        byte[] scratch = TL_COPY_BUF.get();
        int remaining = len;
        long src = address;
        while (remaining > 0) {
            int chunk = Math.min(remaining, scratch.length);
            allocator.copyMemory(src, scratch, 0, chunk);
            out.writeBytes(scratch, 0, chunk);
            src += chunk;
            remaining -= chunk;
        }
    }
}
