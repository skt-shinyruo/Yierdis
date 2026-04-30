package yier.bubu.redis.db.memory.ffm;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.db.memory.foreign.YierdisFfmAccess;
import yier.bubu.redis.offheap.api.OffHeapSlice;

public final class YierdisFfmBytesRefSlice implements OffHeapSlice {
    private static final int COPY_CHUNK_BYTES = 8 * 1024;
    private static final ThreadLocal<byte[]> TL_COPY_BUF =
            ThreadLocal.withInitial(() -> new byte[COPY_CHUNK_BYTES]);

    private final YierdisFfmBytesRef ref;

    public YierdisFfmBytesRefSlice(YierdisFfmBytesRef ref) {
        if (ref == null) {
            throw new IllegalArgumentException("ref must not be null");
        }
        this.ref = ref;
    }

    @Override
    public int length() {
        return ref.length();
    }

    @Override
    public byte getByte(int index) {
        return ref.byteAt(index);
    }

    @Override
    public void getBytes(int index, byte[] dst, int dstOff, int len) {
        if (dst == null) {
            throw new IllegalArgumentException("dst must not be null");
        }
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (dstOff < 0 || dstOff + len > dst.length) {
            throw new IndexOutOfBoundsException();
        }
        if (index < 0 || index + len > ref.length()) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return;
        }
        YierdisFfmAccess.getBytes(ref.span(), index, dst, dstOff, len);
    }

    @Override
    public void writeTo(BytesSink out) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }
        byte[] scratch = TL_COPY_BUF.get();
        int remaining = ref.length();
        int offset = 0;
        while (remaining > 0) {
            int chunk = Math.min(remaining, scratch.length);
            YierdisFfmAccess.getBytes(ref.span(), offset, scratch, 0, chunk);
            out.writeBytes(scratch, 0, chunk);
            offset += chunk;
            remaining -= chunk;
        }
    }
}
