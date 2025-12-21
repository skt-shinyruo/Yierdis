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

    // Hard upper bounds for user-controlled inputs (DoS protection).
    private static final int DEFAULT_MAX_BULK_BYTES = 64 * 1024 * 1024; // 64 MiB
    private static final int DEFAULT_MAX_ARRAY_LEN = 1024;
    private static final int DEFAULT_MAX_NESTING_DEPTH = 64;
    private static final int DEFAULT_MAX_LINE_BYTES = 1024;

    private final int maxBulkBytes;
    private final int maxArrayLen;
    private final int maxNestingDepth;
    private final int maxLineBytes;

    public RespDecoder() {
        this(DEFAULT_MAX_BULK_BYTES, DEFAULT_MAX_ARRAY_LEN, DEFAULT_MAX_NESTING_DEPTH, DEFAULT_MAX_LINE_BYTES);
    }

    RespDecoder(int maxBulkBytes, int maxArrayLen, int maxNestingDepth, int maxLineBytes) {
        this.maxBulkBytes = requirePositive(maxBulkBytes, "maxBulkBytes");
        this.maxArrayLen = requirePositive(maxArrayLen, "maxArrayLen");
        this.maxNestingDepth = requirePositive(maxNestingDepth, "maxNestingDepth");
        this.maxLineBytes = requirePositive(maxLineBytes, "maxLineBytes");
    }

    @Override
    protected void decode(io.netty.channel.ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        for (; ; ) {
            int startIdx = in.readerIndex();
            RespObject obj = tryDecodeOne(in, 0);
            if (obj == null) {
                in.readerIndex(startIdx);
                return;
            }
            out.add(obj);
        }
    }

    private RespObject tryDecodeOne(ByteBuf in, int nestingDepth) {
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
                if (len > maxBulkBytes) {
                    throw new IllegalArgumentException("Protocol error: bulk length too large");
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
                if (nestingDepth >= maxNestingDepth) {
                    throw new IllegalArgumentException("Protocol error: nested arrays too deep");
                }
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
                if (count > maxArrayLen) {
                    throw new IllegalArgumentException("Protocol error: array length too large");
                }
                List<RespObject> items = new ArrayList<>(Math.min(count, 16));
                for (int i = 0; i < count; i++) {
                    RespObject item = tryDecodeOne(in, nestingDepth + 1);
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
    private String readLine(ByteBuf in) {
        int start = in.readerIndex();
        int end = indexOfCrlf(in, maxLineBytes);
        if (end < 0) {
            if (in.writerIndex() - start > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            return null;
        }

        int len = end - start;
        if (len > maxLineBytes) {
            throw new IllegalArgumentException("Protocol error: line too long");
        }
        byte[] buf = new byte[len];
        in.readBytes(buf);
        // consume CRLF
        in.skipBytes(2);
        return new String(buf, StandardCharsets.UTF_8);
    }

    private static int indexOfCrlf(ByteBuf in, int maxLineBytes) {
        int start = in.readerIndex();
        int maxCrlfStart = start + maxLineBytes;
        int scanLimit = Math.min(in.writerIndex() - 1, maxCrlfStart + 1);
        for (int i = start; i < scanLimit; i++) {
            if (in.getByte(i) == CR && in.getByte(i + 1) == LF) {
                return i;
            }
        }
        return -1;
    }

    private static int requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }
}
