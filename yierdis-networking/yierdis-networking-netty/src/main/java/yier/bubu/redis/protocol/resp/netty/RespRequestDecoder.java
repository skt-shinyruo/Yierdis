package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import yier.bubu.redis.protocol.resp.InlineCommandParser;
import yier.bubu.redis.protocol.resp.RespCommandRequest;

import java.util.List;

public final class RespRequestDecoder extends ByteToMessageDecoder {
    private static final byte CR = (byte) '\r';
    private static final byte LF = (byte) '\n';
    private static final byte ARRAY = (byte) '*';
    private static final byte BULK = (byte) '$';

    private enum State {
        READ_COMMAND,
        CLOSING
    }

    private final int maxBulkBytes;
    private final int maxArgs;
    private final int maxInlineBytes;
    private State state = State.READ_COMMAND;

    public RespRequestDecoder(int maxBulkBytes, int maxArgs, int maxInlineBytes) {
        this(maxBulkBytes, maxArgs, maxInlineBytes, Math.max(1024, maxBulkBytes + maxInlineBytes));
    }

    public RespRequestDecoder(int maxBulkBytes, int maxArgs, int maxInlineBytes, int maxDiscardBytes) {
        this.maxBulkBytes = Math.max(0, maxBulkBytes);
        this.maxArgs = Math.max(0, maxArgs);
        this.maxInlineBytes = Math.max(0, maxInlineBytes);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (ctx == null || in == null || out == null) {
            return;
        }
        if (state == State.CLOSING) {
            in.readerIndex(in.writerIndex());
            return;
        }
        for (; ; ) {
            if (!in.isReadable()) {
                return;
            }

            int commandStart = in.readerIndex();
            byte first = in.getByte(commandStart);
            ParseResult result = first == ARRAY ? tryReadArray(in, out) : tryReadInline(in, out);
            if (result == ParseResult.NEED_MORE) {
                in.readerIndex(commandStart);
                return;
            }
            if (result == ParseResult.ERROR) {
                if (shouldCloseAfterReply(out)) {
                    state = State.CLOSING;
                    in.readerIndex(in.writerIndex());
                    return;
                }
                continue;
            }
        }
    }

    private ParseResult tryReadArray(ByteBuf in, List<Object> out) {
        int lineStart = in.readerIndex();
        int lf = findCrlfLine(in, out, "ERR Protocol error: invalid multibulk length");
        if (lf == Integer.MIN_VALUE) {
            return ParseResult.NEED_MORE;
        }
        if (lf < 0) {
            return ParseResult.ERROR;
        }

        Long argcValue = parseInteger(in, lineStart + 1, lf - 1);
        if (argcValue == null || argcValue < 0 || argcValue > Integer.MAX_VALUE) {
            emitProtocolError(out, "ERR Protocol error: invalid multibulk length", true);
            return ParseResult.ERROR;
        }
        int argc = argcValue.intValue();
        if (maxArgs > 0 && argc > maxArgs) {
            emitProtocolError(out, "ERR Protocol error: too many arguments", true);
            return ParseResult.ERROR;
        }

        byte[][] argv = new byte[argc][];
        int retainedBytes = 0;
        for (int i = 0; i < argc; i++) {
            if (!in.isReadable()) {
                return ParseResult.NEED_MORE;
            }
            int bulkLineStart = in.readerIndex();
            int bulkLf = findCrlfLine(in, out, "ERR Protocol error: invalid bulk length");
            if (bulkLf == Integer.MIN_VALUE) {
                return ParseResult.NEED_MORE;
            }
            if (bulkLf < 0) {
                return ParseResult.ERROR;
            }
            if (in.getByte(bulkLineStart) != BULK) {
                emitProtocolError(out, "ERR Protocol error: expected '$', got other", true);
                state = State.CLOSING;
                return ParseResult.ERROR;
            }

            Long lenValue = parseInteger(in, bulkLineStart + 1, bulkLf - 1);
            if (lenValue == null || lenValue < 0 || lenValue > Integer.MAX_VALUE) {
                emitProtocolError(out, "ERR Protocol error: invalid bulk length", true);
                state = State.CLOSING;
                return ParseResult.ERROR;
            }
            int len = lenValue.intValue();
            if (maxBulkBytes > 0 && len > maxBulkBytes) {
                emitProtocolError(out, "ERR Protocol error: invalid bulk length", true);
                state = State.CLOSING;
                return ParseResult.ERROR;
            }
            if (in.readableBytes() < len + 2) {
                return ParseResult.NEED_MORE;
            }

            byte[] arg = new byte[len];
            in.readBytes(arg);
            byte cr = in.readByte();
            byte lfByte = in.readByte();
            if (cr != CR || lfByte != LF) {
                emitProtocolError(out, "ERR Protocol error: invalid bulk string terminator", true);
                state = State.CLOSING;
                return ParseResult.ERROR;
            }
            argv[i] = arg;
            retainedBytes = saturatedAdd(retainedBytes, len);
        }

        out.add(RespCommandRequest.wrapReadOnly(argv, retainedBytes));
        return ParseResult.EMITTED;
    }

    private ParseResult tryReadInline(ByteBuf in, List<Object> out) {
        int lineStart = in.readerIndex();
        int lf = findCrlfLine(in, out, "ERR Protocol error: invalid inline command");
        if (lf == Integer.MIN_VALUE) {
            return ParseResult.NEED_MORE;
        }
        if (lf < 0) {
            return ParseResult.ERROR;
        }

        int lineEnd = lf - 1;
        if (lineEnd <= lineStart || isBlank(in, lineStart, lineEnd)) {
            return ParseResult.EMITTED;
        }

        try {
            byte[] line = new byte[lineEnd - lineStart];
            in.getBytes(lineStart, line);
            InlineCommandParser.Decoded decoded = InlineCommandParser.parseUnlimited(line, 0, line.length);
            if (maxArgs > 0 && decoded.argc() > maxArgs) {
                emitProtocolError(out, "ERR Protocol error: too many arguments", true);
                state = State.CLOSING;
                return ParseResult.ERROR;
            }
            out.add(RespCommandRequest.wrapReadOnly(decoded.copyArgs(), decoded.retainedBytes()));
            return ParseResult.EMITTED;
        } catch (IllegalArgumentException e) {
            emitProtocolError(out, "ERR Protocol error: invalid inline command", true);
            state = State.CLOSING;
            return ParseResult.ERROR;
        }
    }

    /**
     * Returns LF index, Integer.MIN_VALUE for incomplete data, or -1 for a consumed protocol error.
     */
    private int findCrlfLine(ByteBuf in, List<Object> out, String errorMessage) {
        int start = in.readerIndex();
        int lfDistance = in.bytesBefore(LF);
        if (lfDistance < 0) {
            if (maxInlineBytes > 0 && in.readableBytes() > maxInlineBytes) {
                emitProtocolError(out, errorMessage, true);
                in.readerIndex(in.writerIndex());
                state = State.CLOSING;
                return -1;
            }
            return Integer.MIN_VALUE;
        }

        if (maxInlineBytes > 0 && lfDistance + 1 > maxInlineBytes) {
            in.readerIndex(start + lfDistance + 1);
            emitProtocolError(out, errorMessage, true);
            state = State.CLOSING;
            return -1;
        }

        int lf = start + lfDistance;
        if (lf == start || in.getByte(lf - 1) != CR) {
            in.readerIndex(lf + 1);
            emitProtocolError(out, errorMessage, true);
            state = State.CLOSING;
            return -1;
        }
        in.readerIndex(lf + 1);
        return lf;
    }

    private static Long parseInteger(ByteBuf in, int start, int endExclusive) {
        if (start >= endExclusive) {
            return null;
        }
        boolean negative = false;
        int i = start;
        if (in.getByte(i) == '-') {
            negative = true;
            i++;
            if (i >= endExclusive) {
                return null;
            }
        }

        long value = 0L;
        for (; i < endExclusive; i++) {
            int ch = in.getByte(i) & 0xFF;
            if (ch < '0' || ch > '9') {
                return null;
            }
            value = value * 10L + (ch - '0');
            if (value > Integer.MAX_VALUE) {
                return null;
            }
        }
        return negative ? -value : value;
    }

    private static boolean isBlank(ByteBuf in, int start, int endExclusive) {
        for (int i = start; i < endExclusive; i++) {
            if (!isWhitespace(in.getByte(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isWhitespace(byte ch) {
        return ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n';
    }

    private static int saturatedAdd(int current, int len) {
        long next = (long) Math.max(0, current) + Math.max(0, len);
        return next >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) next;
    }

    private static void emitProtocolError(List<Object> out, String message, boolean closeAfterReply) {
        out.add(new RespProtocolError(message, closeAfterReply));
    }

    private static boolean shouldCloseAfterReply(List<Object> out) {
        if (out == null || out.isEmpty()) {
            return false;
        }
        Object last = out.get(out.size() - 1);
        return last instanceof RespProtocolError error && error.closeAfterReply();
    }

    private enum ParseResult {
        NEED_MORE,
        EMITTED,
        ERROR
    }
}
