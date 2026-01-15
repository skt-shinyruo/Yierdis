package yier.bubu.redis.db.offheap.netty;

import io.netty.buffer.ByteBuf;
import yier.bubu.redis.db.offheap.api.YierdisDirectBytesSink;

public final class YierdisNettyByteBufSink implements YierdisDirectBytesSink {
    private final ByteBuf buf;

    public YierdisNettyByteBufSink(ByteBuf buf) {
        if (buf == null) {
            throw new IllegalArgumentException("buf must not be null");
        }
        this.buf = buf;
    }

    public ByteBuf unwrap() {
        return buf;
    }

    @Override
    public void writeBytes(byte[] src, int srcIndex, int len) {
        buf.writeBytes(src, srcIndex, len);
    }

    @Override
    public void ensureWritable(int len) {
        buf.ensureWritable(len);
    }

    @Override
    public int writerIndex() {
        return buf.writerIndex();
    }

    @Override
    public void writerIndex(int writerIndex) {
        buf.writerIndex(writerIndex);
    }

    @Override
    public boolean hasMemoryAddress() {
        return buf.hasMemoryAddress();
    }

    @Override
    public long memoryAddress() {
        if (!buf.hasMemoryAddress()) {
            throw new UnsupportedOperationException("ByteBuf has no memory address");
        }
        return buf.memoryAddress();
    }
}

