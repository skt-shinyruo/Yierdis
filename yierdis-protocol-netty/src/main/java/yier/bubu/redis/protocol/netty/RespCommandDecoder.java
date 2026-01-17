package yier.bubu.redis.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespCommandBuilder;
import yier.bubu.redis.protocol.RespInlineCommandParser;

import java.util.List;

/**
 * Fast path decoder for Redis client requests.
 * <p>
 * Supports the common command form: an array of bulk strings (RESP2).
 * It avoids building generic {@link RespObject} trees.
 */
public final class RespCommandDecoder extends ByteToMessageDecoder {
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    // Hard upper bounds for user-controlled inputs (DoS protection).
    private static final int DEFAULT_MAX_BULK_BYTES = 64 * 1024 * 1024; // 64 MiB
    private static final int DEFAULT_MAX_ARGS = 1024;
    private static final int DEFAULT_MAX_LINE_BYTES = 1024;

    private final int maxBulkBytes;
    private final int maxArgs;
    private final int maxLineBytes;

    public RespCommandDecoder() {
        this(DEFAULT_MAX_BULK_BYTES, DEFAULT_MAX_ARGS, DEFAULT_MAX_LINE_BYTES);
    }

    public RespCommandDecoder(int maxBulkBytes, int maxArgs, int maxLineBytes) {
        this.maxBulkBytes = requirePositive(maxBulkBytes, "maxBulkBytes");
        this.maxArgs = requirePositive(maxArgs, "maxArgs");
        this.maxLineBytes = requirePositive(maxLineBytes, "maxLineBytes");
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        for (; ; ) {
            int startIdx = in.readerIndex();
            RespCommand cmd = tryDecodeOne(in);
            if (cmd == null) {
                in.readerIndex(startIdx);
                return;
            }
            out.add(cmd);
        }
    }

    private RespCommand tryDecodeOne(ByteBuf in) {
        int startIdx = in.readerIndex();
        if (!in.isReadable()) {
            return null;
        }

        byte first = in.getByte(startIdx);
        if (first == '*') {
            return decodeArrayOfBulkStrings(in, startIdx);
        }
        // Avoid treating RESP replies as inline commands; keep the old behavior for these prefixes.
        if (first == '+' || first == '-' || first == ':' || first == '$') {
            throw new IllegalArgumentException("Protocol error: expected array");
        }
        return decodeInlineCommand(in, startIdx);
    }

    private RespCommand decodeArrayOfBulkStrings(ByteBuf in, int startIdx) {
        in.readByte(); // consume '*'

        int argcLineStart = in.readerIndex();
        int argcLineEnd = indexOfCrlf(in, argcLineStart, maxLineBytes);
        if (argcLineEnd < 0) {
            if (in.writerIndex() - argcLineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            in.readerIndex(startIdx);
            return null;
        }

        int argc = parseIntAscii(in, argcLineStart, argcLineEnd);
        if (argc < 0) {
            throw new IllegalArgumentException("Protocol error: invalid array length");
        }
        if (argc > maxArgs) {
            throw new IllegalArgumentException("Protocol error: array length too large");
        }

        in.readerIndex(argcLineEnd + 2); // consume CRLF

        RespCommand cmd = RespCommandBuilder.acquire(argc);
        try {
            for (int i = 0; i < argc; i++) {
                if (!in.isReadable()) {
                    cmd.close();
                    in.readerIndex(startIdx);
                    return null;
                }

                byte bulkPrefix = in.readByte();
                if (bulkPrefix != '$') {
                    throw new IllegalArgumentException("Protocol error: expected bulk string");
                }

                int lenLineStart = in.readerIndex();
                int lenLineEnd = indexOfCrlf(in, lenLineStart, maxLineBytes);
                if (lenLineEnd < 0) {
                    if (in.writerIndex() - lenLineStart > maxLineBytes + 2) {
                        throw new IllegalArgumentException("Protocol error: line too long");
                    }
                    cmd.close();
                    in.readerIndex(startIdx);
                    return null;
                }

                int len = parseIntAscii(in, lenLineStart, lenLineEnd);
                if (len == -1) {
                    // Null bulk string.
                    in.readerIndex(lenLineEnd + 2);
                    RespCommandBuilder.setArgNull(cmd, i);
                    continue;
                }
                if (len < -1) {
                    throw new IllegalArgumentException("Protocol error: invalid bulk length");
                }
                if (len > maxBulkBytes) {
                    throw new IllegalArgumentException("Protocol error: bulk length too large");
                }

                int dataStart = lenLineEnd + 2;
                int dataEnd = dataStart + len;
                int end = dataEnd + 2;
                if (in.writerIndex() < end) {
                    cmd.close();
                    in.readerIndex(startIdx);
                    return null;
                }

                if (in.getByte(dataEnd) != CR || in.getByte(dataEnd + 1) != LF) {
                    throw new IllegalArgumentException("Protocol error: bad bulk string CRLF");
                }

                RespCommandBuilder.setArgSlice(cmd, i, dataStart - startIdx, len);
                in.readerIndex(end);
            }

            int endIdx = in.readerIndex();
            ByteBuf frameBuf = in.retainedSlice(startIdx, endIdx - startIdx);
            NettyRespFrame frame = new NettyRespFrame(frameBuf);
            boolean ok = false;
            try {
                RespCommandBuilder.setFrame(cmd, frame);
                ok = true;
            } finally {
                if (!ok) {
                    frame.close();
                }
            }
            return cmd;
        } catch (RuntimeException e) {
            cmd.close();
            throw e;
        }
    }

    private RespCommand decodeInlineCommand(ByteBuf in, int startIdx) {
        int lineEnd = indexOfCrlf(in, startIdx, maxLineBytes);
        if (lineEnd < 0) {
            if (in.writerIndex() - startIdx > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            in.readerIndex(startIdx);
            return null;
        }

        // Inline command parsing follows Redis-like rules (sdssplitargs) and is shared with CLI parsing.
        // Since escapes require decoding, inline commands are materialized into a small heap buffer.
        int inputLen = lineEnd - startIdx;
        byte[] input = new byte[inputLen];
        in.getBytes(startIdx, input);
        RespInlineCommandParser.Decoded decoded = RespInlineCommandParser.parse(input, 0, inputLen, maxArgs);

        // Consume the line (+ CRLF) before we slice; this keeps the control flow aligned with the RESP path.
        in.readerIndex(lineEnd + 2);

        RespCommand cmd = RespCommandBuilder.acquire(decoded.argc());
        for (int arg = 0; arg < decoded.argc(); arg++) {
            RespCommandBuilder.setArgSlice(cmd, arg, decoded.offset(arg), decoded.length(arg));
        }

        ByteBuf frameBuf = Unpooled.wrappedBuffer(decoded.decoded(), 0, decoded.decodedLen());
        NettyRespFrame frame = new NettyRespFrame(frameBuf);
        boolean ok = false;
        try {
            RespCommandBuilder.setFrame(cmd, frame);
            ok = true;
        } finally {
            if (!ok) {
                frame.close();
            }
        }
        return cmd;
    }

    private static int indexOfCrlf(ByteBuf in, int start, int maxLineBytes) {
        int maxCrlfStart = start + maxLineBytes;
        int scanLimit = Math.min(in.writerIndex() - 1, maxCrlfStart + 1);
        for (int i = start; i < scanLimit; i++) {
            if (in.getByte(i) == CR && in.getByte(i + 1) == LF) {
                return i;
            }
        }
        return -1;
    }

    private static int parseIntAscii(ByteBuf in, int start, int end) {
        int i = start;
        while (i < end && isSpace(in.getByte(i))) {
            i++;
        }
        int j = end;
        while (j > i && isSpace(in.getByte(j - 1))) {
            j--;
        }
        if (i >= j) {
            throw new IllegalArgumentException("Protocol error: empty integer line");
        }

        boolean negative = false;
        byte b = in.getByte(i);
        if (b == '-' || b == '+') {
            negative = b == '-';
            i++;
            if (i >= j) {
                throw new IllegalArgumentException("Protocol error: invalid integer line");
            }
        }

        int result = 0;
        while (i < j) {
            int digit = (in.getByte(i++) & 0xFF) - '0';
            if (digit < 0 || digit > 9) {
                throw new IllegalArgumentException("Protocol error: invalid integer line");
            }
            if (result > (Integer.MAX_VALUE - digit) / 10) {
                throw new IllegalArgumentException("Protocol error: integer out of range");
            }
            result = result * 10 + digit;
        }

        return negative ? -result : result;
    }

    private static boolean isSpace(byte b) {
        return b == ' ' || b == '\t';
    }

    private static int requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }
}
