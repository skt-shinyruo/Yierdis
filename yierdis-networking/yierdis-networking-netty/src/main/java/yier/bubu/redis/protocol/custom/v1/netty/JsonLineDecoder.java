package yier.bubu.redis.protocol.custom.v1.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.TooLongFrameException;

import java.util.List;

/**
 * NDJSON-style line decoder: splits inbound bytes on {@code '\n'}.
 * <p>
 * The emitted line is a heap {@code byte[]} without the trailing {@code '\n'} (and optional trailing {@code '\r'}).
 */
public final class JsonLineDecoder extends ByteToMessageDecoder {
    private static final byte LF = (byte) '\n';

    private final int maxLineBytes;

    public JsonLineDecoder(int maxLineBytes) {
        this.maxLineBytes = Math.max(0, maxLineBytes);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (ctx == null || in == null || out == null) {
            return;
        }

        int start = in.readerIndex();
        int end = in.writerIndex();
        int lfIdx = -1;
        for (int i = start; i < end; i++) {
            if (in.getByte(i) == LF) {
                lfIdx = i;
                break;
            }
        }

        if (lfIdx < 0) {
            if (maxLineBytes > 0 && in.readableBytes() > maxLineBytes) {
                throw new TooLongFrameException("line too long");
            }
            return;
        }

        int len = lfIdx - start;
        if (maxLineBytes > 0 && len > maxLineBytes) {
            in.readerIndex(lfIdx + 1);
            throw new TooLongFrameException("line too long");
        }

        byte[] line = new byte[len];
        in.readBytes(line);
        in.readByte(); // consume LF
        if (len > 0 && line[len - 1] == '\r') {
            byte[] trimmed = new byte[len - 1];
            System.arraycopy(line, 0, trimmed, 0, len - 1);
            out.add(trimmed);
            return;
        }
        out.add(line);
    }
}

