package yier.bubu.redis.db.offheap.api;

import yier.bubu.redis.bytes.DirectBytesSink;

import java.nio.ByteBuffer;

public final class YierdisByteBufferSink implements DirectBytesSink {
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
        if (len == 0) {
            return;
        }
        ensureWritable(len);
        buf.put(src, srcIndex, len);
    }

    @Override
    public void ensureWritable(int len) {
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (buf.remaining() < len) {
            throw new IndexOutOfBoundsException("ByteBuffer overflow");
        }
    }

    @Override
    public int writerIndex() {
        return buf.position();
    }

    @Override
    public void writerIndex(int writerIndex) {
        if (writerIndex < 0 || writerIndex > buf.limit()) {
            throw new IndexOutOfBoundsException("writerIndex out of bounds");
        }
        buf.position(writerIndex);
    }

    @Override
    public boolean hasMemoryAddress() {
        // 不暴露 ByteBuffer address：避免依赖 JDK 私有 API。
        return false;
    }

    @Override
    public long memoryAddress() {
        throw new UnsupportedOperationException("memoryAddress not supported");
    }
}
