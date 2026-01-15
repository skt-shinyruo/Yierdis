package yier.bubu.redis.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.Arrays;
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

    RespCommandDecoder(int maxBulkBytes, int maxArgs, int maxLineBytes) {
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

        RespCommand cmd = RespCommand.acquire(argc);
        try {
            for (int i = 0; i < argc; i++) {
                if (!in.isReadable()) {
                    cmd.recycle();
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
                    cmd.recycle();
                    in.readerIndex(startIdx);
                    return null;
                }

                int len = parseIntAscii(in, lenLineStart, lenLineEnd);
                if (len == -1) {
                    // Null bulk string.
                    in.readerIndex(lenLineEnd + 2);
                    cmd.setArgNull(i);
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
                    cmd.recycle();
                    in.readerIndex(startIdx);
                    return null;
                }

                if (in.getByte(dataEnd) != CR || in.getByte(dataEnd + 1) != LF) {
                    throw new IllegalArgumentException("Protocol error: bad bulk string CRLF");
                }

                cmd.setArgSlice(i, dataStart - startIdx, len);
                in.readerIndex(end);
            }

            int endIdx = in.readerIndex();
            ByteBuf frame = in.retainedSlice(startIdx, endIdx - startIdx);
            boolean ok = false;
            try {
                cmd.setFrame(frame);
                ok = true;
            } finally {
                if (!ok) {
                    frame.release();
                }
            }
            return cmd;
        } catch (RuntimeException e) {
            cmd.recycle();
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

        // Inline command parsing follows Redis-like rules (sdssplitargs):
        // - Split on space/tab
        // - Supports double/single quotes inside tokens
        // - Double quotes: backslash escapes + optional \\xHH hex byte escapes
        // - Single quotes: supports escaping \\' only
        //
        // Since escapes require decoding, inline commands are materialized into a small heap buffer.
        int[] offsets = new int[16];
        int[] lengths = new int[16];
        int argc = 0;

        int inputLen = lineEnd - startIdx;
        byte[] decoded = new byte[inputLen];
        int outPos = 0;

        int p = startIdx;
        while (true) {
            while (p < lineEnd && isSpace(in.getByte(p))) {
                p++;
            }
            if (p >= lineEnd) {
                break;
            }

            boolean inq = false;
            boolean insq = false;
            int tokenStart = outPos;

            while (p < lineEnd) {
                byte b = in.getByte(p);
                if (inq) {
                    if (b == '\\'
                            && p + 3 < lineEnd
                            && in.getByte(p + 1) == 'x'
                            && isHexDigit(in.getByte(p + 2))
                            && isHexDigit(in.getByte(p + 3))) {
                        decoded[outPos++] = (byte) ((hexDigitToInt(in.getByte(p + 2)) << 4)
                                | hexDigitToInt(in.getByte(p + 3)));
                        p += 4;
                        continue;
                    }
                    if (b == '\\' && p + 1 < lineEnd) {
                        p++;
                        decoded[outPos++] = decodeEscapedChar(in.getByte(p));
                        p++;
                        continue;
                    }
                    if (b == '"') {
                        // Closing quote must be followed by whitespace or end-of-line.
                        if (p + 1 < lineEnd && !isSpace(in.getByte(p + 1))) {
                            throw new IllegalArgumentException("Protocol error: invalid inline command");
                        }
                        inq = false;
                        p++;
                        break;
                    }
                    decoded[outPos++] = b;
                    p++;
                    continue;
                }

                if (insq) {
                    if (b == '\\' && p + 1 < lineEnd && in.getByte(p + 1) == '\'') {
                        decoded[outPos++] = (byte) '\'';
                        p += 2;
                        continue;
                    }
                    if (b == '\'') {
                        // Closing quote must be followed by whitespace or end-of-line.
                        if (p + 1 < lineEnd && !isSpace(in.getByte(p + 1))) {
                            throw new IllegalArgumentException("Protocol error: invalid inline command");
                        }
                        insq = false;
                        p++;
                        break;
                    }
                    decoded[outPos++] = b;
                    p++;
                    continue;
                }

                if (isSpace(b)) {
                    break;
                }
                if (b == '"') {
                    inq = true;
                    p++;
                    continue;
                }
                if (b == '\'') {
                    insq = true;
                    p++;
                    continue;
                }
                decoded[outPos++] = b;
                p++;
            }

            if (inq || insq) {
                throw new IllegalArgumentException("Protocol error: unbalanced quotes in inline command");
            }

            int len = outPos - tokenStart;
            if (argc >= maxArgs) {
                throw new IllegalArgumentException("Protocol error: array length too large");
            }
            if (argc == offsets.length) {
                int next = offsets.length << 1;
                offsets = Arrays.copyOf(offsets, next);
                lengths = Arrays.copyOf(lengths, next);
            }
            offsets[argc] = tokenStart;
            lengths[argc] = len;
            argc++;
        }

        if (argc == 0) {
            throw new IllegalArgumentException("Protocol error: empty inline command");
        }

        // Consume the line (+ CRLF) before we slice; this keeps the control flow aligned with the RESP path.
        in.readerIndex(lineEnd + 2);

        RespCommand cmd = RespCommand.acquire(argc);
        for (int arg = 0; arg < argc; arg++) {
            cmd.setArgSlice(arg, offsets[arg], lengths[arg]);
        }

        ByteBuf frame = Unpooled.wrappedBuffer(decoded, 0, outPos);
        boolean ok = false;
        try {
            cmd.setFrame(frame);
            ok = true;
        } finally {
            if (!ok) {
                frame.release();
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

    private static byte decodeEscapedChar(byte b) {
        return switch (b) {
            case 'n' -> (byte) '\n';
            case 'r' -> (byte) '\r';
            case 't' -> (byte) '\t';
            case 'b' -> (byte) '\b';
            case 'a' -> (byte) '\u0007';
            default -> b;
        };
    }

    private static boolean isHexDigit(byte b) {
        return (b >= '0' && b <= '9')
                || (b >= 'a' && b <= 'f')
                || (b >= 'A' && b <= 'F');
    }

    private static int hexDigitToInt(byte b) {
        if (b >= '0' && b <= '9') {
            return b - '0';
        }
        if (b >= 'a' && b <= 'f') {
            return 10 + (b - 'a');
        }
        if (b >= 'A' && b <= 'F') {
            return 10 + (b - 'A');
        }
        return 0;
    }

    private static int requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }
}
