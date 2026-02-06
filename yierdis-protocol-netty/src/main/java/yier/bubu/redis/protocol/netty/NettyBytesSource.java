package yier.bubu.redis.protocol.netty;

// ByteBuf 到 BytesSource 的轻量适配：用于把 Netty streaming buffer 映射到 protocol 层的 wire skipper（SSOT）。

import io.netty.buffer.ByteBuf;
import yier.bubu.redis.bytes.BytesSource;

final class NettyBytesSource implements BytesSource {
    private final ByteBuf buf;
    private final int baseOffset;

    NettyBytesSource(ByteBuf buf, int baseOffset) {
        if (buf == null) {
            throw new IllegalArgumentException("buf must not be null");
        }
        if (baseOffset < 0) {
            throw new IllegalArgumentException("baseOffset must be >= 0");
        }
        this.buf = buf;
        this.baseOffset = baseOffset;
    }

    @Override
    public byte getByte(int index) {
        return buf.getByte(baseOffset + index);
    }

    @Override
    public void getBytes(int index, byte[] dst, int dstOff, int len) {
        buf.getBytes(baseOffset + index, dst, dstOff, len);
    }
}

