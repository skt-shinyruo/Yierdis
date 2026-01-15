package yier.bubu.redis.db.offheap.api;

import java.nio.ByteBuffer;

public final class YierdisByteBufferSink implements YierdisBytesSink {
    private final ByteBuffer buf;

    public YierdisByteBufferSink(ByteBuffer buf) {
        if (buf == null) {
            throw new IllegalArgumentException("buf must not be null");
        }
        this.buf = buf;
    }

    public ByteBuffer buffer() {
        return buf;
    }

    @Override
    public void writeBytes(byte[] src, int srcIndex, int len) {
        if (src == null) {
            throw new IllegalArgumentException("src must not be null");
        }
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (srcIndex < 0 || srcIndex + len > src.length) {
            throw new IndexOutOfBoundsException();
        }
        if (buf.remaining() < len) {
            throw new IndexOutOfBoundsException("ByteBuffer overflow");
        }
        if (len == 0) {
            return;
        }
        buf.put(src, srcIndex, len);
    }
}

