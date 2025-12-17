package yier.bubu.redis.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A small RESP2 decoder for Redis clients.
 * <p>
 * It is intentionally minimal and focuses on correctness over extreme performance.
 */
public final class RespDecoder extends ByteToMessageDecoder {
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    @Override
    protected void decode(io.netty.channel.ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        for (; ; ) {
            int startIdx = in.readerIndex();
            RespObject obj = tryDecodeOne(in);
            if (obj == null) {
                in.readerIndex(startIdx);
                return;
            }
            out.add(obj);
        }
    }

    private RespObject tryDecodeOne(ByteBuf in) {
        int startIdx = in.readerIndex();
        if (!in.isReadable()) {
            return null;
        }

        byte prefix = in.readByte();
        switch (prefix) {
            case '+': {
                String line = readLine(in);
                if (line == null) {
                    in.readerIndex(startIdx);
                    return null;
                }
                return RespSimpleString.of(line);
            }
            case '-': {
                String line = readLine(in);
                if (line == null) {
                    in.readerIndex(startIdx);
                    return null;
                }
                return RespError.of(line);
            }
            case ':': {
                String line = readLine(in);
                if (line == null) {
                    in.readerIndex(startIdx);
                    return null;
                }
                return RespInteger.of(Long.parseLong(line.trim()));
            }
            case '$': {
                String line = readLine(in);
                if (line == null) {
                    in.readerIndex(startIdx);
                    return null;
                }
                int len = Integer.parseInt(line.trim());
                if (len == -1) {
                    return RespBulkString.nullString();
                }
                if (len < -1) {
                    throw new IllegalArgumentException("Protocol error: invalid bulk length");
                }
                if (in.readableBytes() < (long) len + 2) {
                    in.readerIndex(startIdx);
                    return null;
                }
                byte[] data = new byte[len];
                in.readBytes(data);
                if (in.readByte() != CR || in.readByte() != LF) {
                    throw new IllegalArgumentException("Protocol error: bad bulk string CRLF");
                }
                return RespBulkString.ofBytes(data);
            }
            case '*': {
                String line = readLine(in);
                if (line == null) {
                    in.readerIndex(startIdx);
                    return null;
                }
                int count = Integer.parseInt(line.trim());
                if (count == -1) {
                    return RespArray.nullArray();
                }
                if (count < -1) {
                    throw new IllegalArgumentException("Protocol error: invalid array length");
                }
                List<RespObject> items = new ArrayList<>(Math.min(count, 16));
                for (int i = 0; i < count; i++) {
                    RespObject item = tryDecodeOne(in);
                    if (item == null) {
                        in.readerIndex(startIdx);
                        return null;
                    }
                    items.add(item);
                }
                return RespArray.of(items);
            }
            default:
                throw new IllegalArgumentException("Protocol error: unknown RESP prefix: " + (char) prefix);
        }
    }

    /**
     * Reads a RESP line (up to CRLF) and returns it without CRLF.
     */
    private static String readLine(ByteBuf in) {
        int start = in.readerIndex();
        int end = indexOfCrlf(in);
        if (end < 0) {
            return null;
        }

        int len = end - start;
        byte[] buf = new byte[len];
        in.readBytes(buf);
        // consume CRLF
        in.skipBytes(2);
        return new String(buf, StandardCharsets.UTF_8);
    }

    private static int indexOfCrlf(ByteBuf in) {
        for (int i = in.readerIndex(); i < in.writerIndex() - 1; i++) {
            if (in.getByte(i) == CR && in.getByte(i + 1) == LF) {
                return i;
            }
        }
        return -1;
    }
}
