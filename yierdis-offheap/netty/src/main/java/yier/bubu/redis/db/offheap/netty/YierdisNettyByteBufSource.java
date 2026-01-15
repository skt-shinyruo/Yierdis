package yier.bubu.redis.db.offheap.netty;

import io.netty.buffer.ByteBuf;
import yier.bubu.redis.db.offheap.api.YierdisBytesSource;

public final class YierdisNettyByteBufSource implements YierdisBytesSource {
    private final ByteBuf buf;

    public YierdisNettyByteBufSource(ByteBuf buf) {
        if (buf == null) {
            throw new IllegalArgumentException("buf must not be null");
        }
        this.buf = buf;
    }

    public ByteBuf unwrap() {
        return buf;
    }

    @Override
    public byte getByte(int index) {
        return buf.getByte(index);
    }

    @Override
    public void getBytes(int index, byte[] dst, int dstOff, int len) {
        buf.getBytes(index, dst, dstOff, len);
    }

    @Override
    public boolean hasMemoryAddress() {
        return buf.hasMemoryAddress();
    }

    @Override
    public long memoryAddress() {
        if (!buf.hasMemoryAddress()) {
            return YierdisBytesSource.super.memoryAddress();
        }
        return buf.memoryAddress();
    }
}

