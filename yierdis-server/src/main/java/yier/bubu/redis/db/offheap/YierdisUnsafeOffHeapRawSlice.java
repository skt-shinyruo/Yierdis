package yier.bubu.redis.db.offheap;

import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapSlice;

/**
 * A raw off-heap slice backed by an absolute address.
 * <p>
 * This slice does not provide ownership/lifecycle management; callers MUST ensure the underlying
 * memory remains valid for the duration of usage (typically within a single command execution).
 */
public final class YierdisUnsafeOffHeapRawSlice implements YierdisOffHeapSlice {
    private static final int COPY_CHUNK_BYTES = 8 * 1024;
    private static final ThreadLocal<byte[]> TL_COPY_BUF =
            ThreadLocal.withInitial(() -> new byte[COPY_CHUNK_BYTES]);

    private final long address;
    private final int len;

    public YierdisUnsafeOffHeapRawSlice(long address, int len) {
        if (address == 0) {
            throw new IllegalArgumentException("address must be != 0");
        }
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
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
        return PlatformDependent.getByte(address + index);
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
        PlatformDependent.copyMemory(address + index, dst, dstOff, readLen);
    }

    @Override
    public void writeTo(ByteBuf out) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }
        if (len == 0) {
            return;
        }

        int before = out.writerIndex();
        out.ensureWritable(len);

        if (out.hasMemoryAddress()) {
            long dstAddr = out.memoryAddress() + before;
            PlatformDependent.copyMemory(address, dstAddr, len);
            out.writerIndex(before + len);
            return;
        }

        byte[] scratch = TL_COPY_BUF.get();
        int remaining = len;
        long src = address;
        while (remaining > 0) {
            int chunk = Math.min(remaining, scratch.length);
            PlatformDependent.copyMemory(src, scratch, 0, chunk);
            out.writeBytes(scratch, 0, chunk);
            src += chunk;
            remaining -= chunk;
        }
    }
}

