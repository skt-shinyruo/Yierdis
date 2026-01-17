package yier.bubu.redis.bytes.netty;

import io.netty.buffer.ByteBuf;
import yier.bubu.redis.bytes.DirectBytesSink;

/**
 * Netty {@link ByteBuf} 的 {@link DirectBytesSink} 适配器。
 * <p>
 * 该类位于 bytes 的 Netty 适配层，用于让 protocol/off-heap 的写出路径保持 Netty-free 抽象，同时在需要时
 * 仍能通过 unwrap 走 ByteBuf fast-path（例如 off-heap slice 的零拷贝/少拷贝写出）。
 */
public final class NettyByteBufSink implements DirectBytesSink {
    private final ByteBuf buf;

    public NettyByteBufSink(ByteBuf buf) {
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

