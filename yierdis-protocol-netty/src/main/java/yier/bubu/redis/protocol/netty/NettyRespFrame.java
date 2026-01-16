package yier.bubu.redis.protocol.netty;

import io.netty.buffer.ByteBuf;

import yier.bubu.redis.protocol.RespFrame;

public final class NettyRespFrame implements RespFrame {
    private ByteBuf buf;
    private final int length;

    public NettyRespFrame(ByteBuf buf) {
        if (buf == null) {
            throw new IllegalArgumentException("buf must not be null");
        }
        this.buf = buf;
        this.length = buf.readableBytes();
    }

    public ByteBuf unwrap() {
        return buf;
    }

    @Override
    public int length() {
        return length;
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
            return RespFrame.super.memoryAddress();
        }
        return buf.memoryAddress();
    }

    @Override
    public void close() {
        if (buf != null) {
            buf.release();
            buf = null;
        }
    }
}
