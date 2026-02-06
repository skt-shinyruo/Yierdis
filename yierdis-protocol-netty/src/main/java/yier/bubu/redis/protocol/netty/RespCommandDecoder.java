package yier.bubu.redis.protocol.netty;

// RESP 请求解码器：支持 RESP2 array-of-bulk-strings 与 inline command（更贴近 Redis：非 '*' 前缀按 inline 处理）。

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespCommandBuilder;
import yier.bubu.redis.protocol.RespInlineCommandParser;
import yier.bubu.redis.protocol.RespLimits;
import yier.bubu.redis.protocol.RespWireSkipper;

import java.util.List;

/**
 * Fast path decoder for Redis client requests.
 * <p>
 * Supports the common command form: an array of bulk strings (RESP2).
 * It avoids building generic {@link RespObject} trees.
 */
public final class RespCommandDecoder extends ByteToMessageDecoder {
    private final int maxBulkBytes;
    private final int maxArgs;
    private final int maxLineBytes;
    private final int maxNestingDepth;

    public RespCommandDecoder() {
        this(RespLimits.DEFAULT_MAX_BULK_BYTES, RespLimits.DEFAULT_MAX_ARGS, RespLimits.DEFAULT_MAX_LINE_BYTES);
    }

    public RespCommandDecoder(int maxBulkBytes, int maxArgs, int maxLineBytes) {
        this.maxBulkBytes = RespDecodingSupport.requirePositive(maxBulkBytes, "maxBulkBytes");
        this.maxArgs = RespDecodingSupport.requirePositive(maxArgs, "maxArgs");
        this.maxLineBytes = RespDecodingSupport.requirePositive(maxLineBytes, "maxLineBytes");
        this.maxNestingDepth = RespDecodingSupport.requirePositive(RespLimits.DEFAULT_MAX_NESTING_DEPTH, "maxNestingDepth");
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

        // RESP3 attributes：允许在 request 前置携带 metadata（忽略 attributes map，仅解析其后真实命令）。
        // 支持 attributes 链式嵌套：|...|...*...
        while (in.isReadable() && in.getByte(in.readerIndex()) == '|') {
            if (!trySkipAttributeMap(in)) {
                in.readerIndex(startIdx);
                return null;
            }
        }

        int cmdStartIdx = in.readerIndex();
        if (!in.isReadable()) {
            in.readerIndex(startIdx);
            return null;
        }

        byte first = in.getByte(cmdStartIdx);
        if (first == '*') {
            return decodeCommandArray(in, cmdStartIdx);
        }
        // Redis 兼容：top-level 非 array 的输入按 inline command 解析（sdssplitargs 风格）。
        // 说明：这会使得某些“非标准 RESP frame”（例如误把 reply 前缀当 request）更像 unknown command，而不是直接 fail-fast。
        if (isInvalidRequestPrefix(first)) {
            throw new IllegalArgumentException("Protocol error: invalid request");
        }
        return decodeInlineCommand(in, cmdStartIdx);
    }

    private static boolean isInvalidRequestPrefix(byte b) {
        // 允许空格/制表符用于 inline command 的前导空白，其余控制字符直接判为协议错误。
        int x = b & 0xFF;
        if (x == ' ' || x == '\t') {
            return false;
        }
        return x < 0x20 || x == 0x7F;
    }

    private RespCommand decodeCommandArray(ByteBuf in, int startIdx) {
        in.readByte(); // consume '*'

        int argcLineStart = in.readerIndex();
        int argcLineEnd = RespDecodingSupport.indexOfCrlf(in, argcLineStart, maxLineBytes);
        if (argcLineEnd < 0) {
            if (in.writerIndex() - argcLineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            in.readerIndex(startIdx);
            return null;
        }

        if (RespDecodingSupport.isSingleCharLine(in, argcLineStart, argcLineEnd, (byte) '?')) {
            in.readerIndex(startIdx);
            return decodeCommandArrayMaterialized(in, startIdx);
        }

        int argc = RespDecodingSupport.parseIntAscii(in, argcLineStart, argcLineEnd);
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

                byte argPrefix = in.readByte();
                switch (argPrefix) {
                    case '$': {
                        if (!tryDecodeBulkStringArg(in, startIdx, cmd, i)) {
                            cmd.close();
                            in.readerIndex(startIdx);
                            return null;
                        }
                        break;
                    }
                    case '_': {
                        if (!tryDecodeNullArg(in, cmd, i)) {
                            cmd.close();
                            in.readerIndex(startIdx);
                            return null;
                        }
                        break;
                    }
                    case '+':
                    case '-':
                    case ':':
                    case ',':
                    case '(': {
                        if (!tryDecodeLineArg(in, startIdx, cmd, i)) {
                            cmd.close();
                            in.readerIndex(startIdx);
                            return null;
                        }
                        break;
                    }
                    case '#': {
                        if (!tryDecodeBooleanArg(in, startIdx, cmd, i)) {
                            cmd.close();
                            in.readerIndex(startIdx);
                            return null;
                        }
                        break;
                    }
                    case '!': {
                        if (!tryDecodeBlobErrorArg(in, startIdx, cmd, i)) {
                            cmd.close();
                            in.readerIndex(startIdx);
                            return null;
                        }
                        break;
                    }
                    case '=': {
                        if (!tryDecodeVerbatimStringArg(in, startIdx, cmd, i)) {
                            cmd.close();
                            in.readerIndex(startIdx);
                            return null;
                        }
                        break;
                    }
                    default:
                        // 命令参数语义仍以“字符串/标量”为主；复杂结构体会引入不确定语义，因此保持拒绝。
                        throw new IllegalArgumentException("Protocol error: expected bulk string or scalar");
                }
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
        } catch (MaterializeCommandException e) {
            cmd.close();
            in.readerIndex(startIdx);
            return decodeCommandArrayMaterialized(in, startIdx);
        } catch (RuntimeException e) {
            cmd.close();
            throw e;
        }
    }

    private RespCommand decodeCommandArrayMaterialized(ByteBuf in, int startIdx) {
        int frameStart = in.readerIndex();
        if (!in.isReadable()) {
            return null;
        }
        if (in.getByte(frameStart) != '*') {
            throw new IllegalStateException("expected array prefix");
        }
        in.readByte(); // consume '*'

        int countLineStart = in.readerIndex();
        int countLineEnd = RespDecodingSupport.indexOfCrlf(in, countLineStart, maxLineBytes);
        if (countLineEnd < 0) {
            if (in.writerIndex() - countLineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            in.readerIndex(startIdx);
            return null;
        }

        if (RespDecodingSupport.isSingleCharLine(in, countLineStart, countLineEnd, (byte) '?')) {
            in.readerIndex(countLineEnd + 2);
            return decodeStreamedCommandArrayMaterialized(in, startIdx);
        }

        int argc = RespDecodingSupport.parseIntAscii(in, countLineStart, countLineEnd);
        if (argc < 0) {
            throw new IllegalArgumentException("Protocol error: invalid array length");
        }
        if (argc > maxArgs) {
            throw new IllegalArgumentException("Protocol error: array length too large");
        }
        in.readerIndex(countLineEnd + 2);
        return decodeFixedCommandArrayMaterialized(in, startIdx, argc);
    }

    private RespCommand decodeFixedCommandArrayMaterialized(ByteBuf in, int startIdx, int argc) {
        RespCommand cmd = RespCommandBuilder.acquire(argc);
        ByteBuf outBuf = in.alloc().buffer(Math.min(256, maxBulkBytes));
        boolean ok = false;
        try {
            for (int i = 0; i < argc; i++) {
                if (!tryDecodeOneArgMaterialized(in, startIdx, cmd, i, outBuf)) {
                    cmd.close();
                    cmd = null;
                    outBuf.release();
                    outBuf = null;
                    in.readerIndex(startIdx);
                    return null;
                }
            }
            NettyRespFrame frame = new NettyRespFrame(outBuf);
            outBuf = null;
            RespCommandBuilder.setFrame(cmd, frame);
            ok = true;
            return cmd;
        } finally {
            if (!ok) {
                if (cmd != null) {
                    cmd.close();
                }
                if (outBuf != null) {
                    outBuf.release();
                }
            }
        }
    }

    private RespCommand decodeStreamedCommandArrayMaterialized(ByteBuf in, int startIdx) {
        ByteBuf outBuf = in.alloc().buffer(Math.min(256, maxBulkBytes));
        int[] offsets = new int[16];
        int[] lens = new int[16];
        int argc = 0;
        boolean ok = false;
        try {
            for (; ; ) {
                if (!in.isReadable()) {
                    outBuf.release();
                    outBuf = null;
                    in.readerIndex(startIdx);
                    return null;
                }

                byte next = in.getByte(in.readerIndex());
                if (next == '.') {
                    if (in.readableBytes() < 3) {
                        outBuf.release();
                        outBuf = null;
                        in.readerIndex(startIdx);
                        return null;
                    }
                    in.readByte();
                    if (in.readByte() != RespDecodingSupport.CR || in.readByte() != RespDecodingSupport.LF) {
                        throw new IllegalArgumentException("Protocol error: bad streamed aggregate end marker");
                    }
                    break;
                }

                if (argc >= maxArgs) {
                    throw new IllegalArgumentException("Protocol error: array length too large");
                }

                if (argc == offsets.length) {
                    int nextCap = offsets.length << 1;
                    int[] newOff = new int[nextCap];
                    int[] newLen = new int[nextCap];
                    System.arraycopy(offsets, 0, newOff, 0, offsets.length);
                    System.arraycopy(lens, 0, newLen, 0, lens.length);
                    offsets = newOff;
                    lens = newLen;
                }

                int argOffsetBefore = outBuf.writerIndex();
                int argLen = tryReadOneArgValueIntoBuffer(in, startIdx, outBuf);
                if (argLen == Integer.MIN_VALUE) {
                    outBuf.release();
                    outBuf = null;
                    in.readerIndex(startIdx);
                    return null;
                }
                if (argLen < 0) {
                    offsets[argc] = 0;
                    lens[argc] = -1;
                } else {
                    offsets[argc] = argOffsetBefore;
                    lens[argc] = argLen;
                }
                argc++;
            }

            RespCommand cmd = RespCommandBuilder.acquire(argc);
            for (int i = 0; i < argc; i++) {
                if (lens[i] < 0) {
                    RespCommandBuilder.setArgNull(cmd, i);
                } else {
                    RespCommandBuilder.setArgSlice(cmd, i, offsets[i], lens[i]);
                }
            }
            NettyRespFrame frame = new NettyRespFrame(outBuf);
            outBuf = null;
            RespCommandBuilder.setFrame(cmd, frame);
            ok = true;
            return cmd;
        } finally {
            if (!ok) {
                if (outBuf != null) {
                    outBuf.release();
                }
            }
        }
    }

    private boolean tryDecodeOneArgMaterialized(ByteBuf in, int frameStartIdx, RespCommand cmd, int argIndex, ByteBuf outBuf) {
        int startIdx = in.readerIndex();
        if (!in.isReadable()) {
            return false;
        }

        int argOffsetBefore = outBuf.writerIndex();
        int argLen = tryReadOneArgValueIntoBuffer(in, frameStartIdx, outBuf);
        if (argLen == Integer.MIN_VALUE) {
            in.readerIndex(startIdx);
            return false;
        }
        if (argLen < 0) {
            RespCommandBuilder.setArgNull(cmd, argIndex);
            return true;
        }
        RespCommandBuilder.setArgSlice(cmd, argIndex, argOffsetBefore, argLen);
        return true;
    }

    /**
     * Parses a single RESP3 scalar/blob argument and appends its payload bytes into {@code outBuf}.
     *
     * @return payload length, {@code -1} for null, or {@code Integer.MIN_VALUE} when more data is required.
     */
    private int tryReadOneArgValueIntoBuffer(ByteBuf in, int frameStartIdx, ByteBuf outBuf) {
        int startIdx = in.readerIndex();
        if (!in.isReadable()) {
            return Integer.MIN_VALUE;
        }

        byte prefix = in.readByte();
        switch (prefix) {
            case '$': {
                int lenLineStart = in.readerIndex();
                int lenLineEnd = RespDecodingSupport.indexOfCrlf(in, lenLineStart, maxLineBytes);
                if (lenLineEnd < 0) {
                    if (in.writerIndex() - lenLineStart > maxLineBytes + 2) {
                        throw new IllegalArgumentException("Protocol error: line too long");
                    }
                    in.readerIndex(startIdx);
                    return Integer.MIN_VALUE;
                }

                if (RespDecodingSupport.isSingleCharLine(in, lenLineStart, lenLineEnd, (byte) '?')) {
                    in.readerIndex(lenLineEnd + 2);
                    return tryReadStreamedBlobIntoBuffer(in, startIdx, outBuf);
                }

                int len = RespDecodingSupport.parseIntAscii(in, lenLineStart, lenLineEnd);
                if (len == -1) {
                    in.readerIndex(lenLineEnd + 2);
                    return -1;
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
                    in.readerIndex(startIdx);
                    return Integer.MIN_VALUE;
                }
                if (in.getByte(dataEnd) != RespDecodingSupport.CR || in.getByte(dataEnd + 1) != RespDecodingSupport.LF) {
                    throw new IllegalArgumentException("Protocol error: bad bulk string CRLF");
                }

                if (len > 0) {
                    outBuf.writeBytes(in, dataStart, len);
                }
                in.readerIndex(end);
                return len;
            }
            case '+':
            case '-':
            case ':':
            case ',':
            case '(': {
                int lineStart = in.readerIndex();
                int lineEnd = RespDecodingSupport.indexOfCrlf(in, lineStart, maxLineBytes);
                if (lineEnd < 0) {
                    if (in.writerIndex() - lineStart > maxLineBytes + 2) {
                        throw new IllegalArgumentException("Protocol error: line too long");
                    }
                    in.readerIndex(startIdx);
                    return Integer.MIN_VALUE;
                }
                if (prefix == ':') {
                    RespDecodingSupport.parseLongAscii(in, lineStart, lineEnd);
                }
                int len = lineEnd - lineStart;
                if (len > 0) {
                    outBuf.writeBytes(in, lineStart, len);
                }
                in.readerIndex(lineEnd + 2);
                return len;
            }
            case '!': {
                int lenLineStart = in.readerIndex();
                int lenLineEnd = RespDecodingSupport.indexOfCrlf(in, lenLineStart, maxLineBytes);
                if (lenLineEnd < 0) {
                    if (in.writerIndex() - lenLineStart > maxLineBytes + 2) {
                        throw new IllegalArgumentException("Protocol error: line too long");
                    }
                    in.readerIndex(startIdx);
                    return Integer.MIN_VALUE;
                }

                if (RespDecodingSupport.isSingleCharLine(in, lenLineStart, lenLineEnd, (byte) '?')) {
                    throw new IllegalArgumentException("Protocol error: invalid blob error length");
                }

                int len = RespDecodingSupport.parseIntAscii(in, lenLineStart, lenLineEnd);
                if (len < 0) {
                    throw new IllegalArgumentException("Protocol error: invalid blob error length");
                }
                if (len > maxBulkBytes) {
                    throw new IllegalArgumentException("Protocol error: bulk length too large");
                }

                int dataStart = lenLineEnd + 2;
                int dataEnd = dataStart + len;
                int end = dataEnd + 2;
                if (in.writerIndex() < end) {
                    in.readerIndex(startIdx);
                    return Integer.MIN_VALUE;
                }
                if (in.getByte(dataEnd) != RespDecodingSupport.CR || in.getByte(dataEnd + 1) != RespDecodingSupport.LF) {
                    throw new IllegalArgumentException("Protocol error: bad bulk string CRLF");
                }

                if (len > 0) {
                    outBuf.writeBytes(in, dataStart, len);
                }
                in.readerIndex(end);
                return len;
            }
            case '#': {
                if (in.readableBytes() < 3) {
                    in.readerIndex(startIdx);
                    return Integer.MIN_VALUE;
                }
                byte v = in.readByte();
                if (v != 't' && v != 'f') {
                    throw new IllegalArgumentException("Protocol error: invalid boolean value");
                }
                if (in.readByte() != RespDecodingSupport.CR || in.readByte() != RespDecodingSupport.LF) {
                    throw new IllegalArgumentException("Protocol error: bad boolean CRLF");
                }
                outBuf.writeByte(v);
                return 1;
            }
            case '_': {
                if (in.readableBytes() < 2) {
                    in.readerIndex(startIdx);
                    return Integer.MIN_VALUE;
                }
                if (in.readByte() != RespDecodingSupport.CR || in.readByte() != RespDecodingSupport.LF) {
                    throw new IllegalArgumentException("Protocol error: bad null CRLF");
                }
                return -1;
            }
            case '=': {
                int lenLineStart = in.readerIndex();
                int lenLineEnd = RespDecodingSupport.indexOfCrlf(in, lenLineStart, maxLineBytes);
                if (lenLineEnd < 0) {
                    if (in.writerIndex() - lenLineStart > maxLineBytes + 2) {
                        throw new IllegalArgumentException("Protocol error: line too long");
                    }
                    in.readerIndex(startIdx);
                    return Integer.MIN_VALUE;
                }

                int len = RespDecodingSupport.parseIntAscii(in, lenLineStart, lenLineEnd);
                if (len < 0) {
                    throw new IllegalArgumentException("Protocol error: invalid verbatim string length");
                }
                if (len > maxBulkBytes) {
                    throw new IllegalArgumentException("Protocol error: bulk length too large");
                }

                int payloadStart = lenLineEnd + 2;
                int payloadEnd = payloadStart + len;
                int end = payloadEnd + 2;
                if (in.writerIndex() < end) {
                    in.readerIndex(startIdx);
                    return Integer.MIN_VALUE;
                }
                if (len < 4 || in.getByte(payloadStart + 3) != (byte) ':') {
                    throw new IllegalArgumentException("Protocol error: invalid verbatim string payload");
                }
                if (in.getByte(payloadEnd) != RespDecodingSupport.CR || in.getByte(payloadEnd + 1) != RespDecodingSupport.LF) {
                    throw new IllegalArgumentException("Protocol error: bad bulk string CRLF");
                }

                int dataLen = len - 4;
                if (dataLen > 0) {
                    outBuf.writeBytes(in, payloadStart + 4, dataLen);
                }
                in.readerIndex(end);
                return dataLen;
            }
            default:
                throw new IllegalArgumentException("Protocol error: expected bulk string or scalar");
        }
    }

    private int tryReadStreamedBlobIntoBuffer(ByteBuf in, int startIdx, ByteBuf outBuf) {
        int total = 0;
        for (; ; ) {
            if (!in.isReadable()) {
                in.readerIndex(startIdx);
                return Integer.MIN_VALUE;
            }
            if (in.readByte() != ';') {
                throw new IllegalArgumentException("Protocol error: invalid streamed blob chunk prefix");
            }

            int lenLineStart = in.readerIndex();
            int lenLineEnd = RespDecodingSupport.indexOfCrlf(in, lenLineStart, maxLineBytes);
            if (lenLineEnd < 0) {
                if (in.writerIndex() - lenLineStart > maxLineBytes + 2) {
                    throw new IllegalArgumentException("Protocol error: line too long");
                }
                in.readerIndex(startIdx);
                return Integer.MIN_VALUE;
            }

            int len;
            try {
                len = RespDecodingSupport.parseIntAscii(in, lenLineStart, lenLineEnd);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Protocol error: invalid streamed blob chunk length");
            }
            if (len < 0) {
                throw new IllegalArgumentException("Protocol error: invalid streamed blob chunk length");
            }
            in.readerIndex(lenLineEnd + 2);
            if (len == 0) {
                return total;
            }
            if (total > maxBulkBytes - len) {
                throw new IllegalArgumentException("Protocol error: bulk length too large");
            }

            int dataStart = in.readerIndex();
            int dataEnd = dataStart + len;
            int end = dataEnd + 2;
            if (in.writerIndex() < end) {
                in.readerIndex(startIdx);
                return Integer.MIN_VALUE;
            }
            if (in.getByte(dataEnd) != RespDecodingSupport.CR || in.getByte(dataEnd + 1) != RespDecodingSupport.LF) {
                throw new IllegalArgumentException("Protocol error: bad bulk string CRLF");
            }

            if (len > 0) {
                outBuf.writeBytes(in, dataStart, len);
            }
            in.readerIndex(end);
            total += len;
        }
    }

    private static final class MaterializeCommandException extends RuntimeException {
        private MaterializeCommandException() {
            super("materialize");
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }

    private static final MaterializeCommandException MATERIALIZE = new MaterializeCommandException();

    private boolean trySkipAttributeMap(ByteBuf in) {
        int startIdx = in.readerIndex();
        int available = in.writerIndex() - startIdx;
        if (available <= 0) {
            return false;
        }

        int endOffset = RespWireSkipper.trySkipAttributeMapOnly(
                new NettyBytesSource(in, startIdx),
                0,
                available,
                maxBulkBytes,
                maxArgs,
                maxNestingDepth,
                maxLineBytes
        );
        if (endOffset < 0) {
            in.readerIndex(startIdx);
            return false;
        }
        in.readerIndex(startIdx + endOffset);
        return true;
    }

    private int trySkipOne(ByteBuf in, int nestingDepth) {
        int startIdx = in.readerIndex();
        if (!in.isReadable()) {
            return -1;
        }
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested arrays too deep");
        }

        byte prefix = in.readByte();
        switch (prefix) {
            case '+':
            case '-':
            case ',':
            case '(': {
                return trySkipLine(in, startIdx);
            }
            case ':': {
                return trySkipInteger(in, startIdx);
            }
            case '#': {
                return trySkipBoolean(in, startIdx);
            }
            case '$': {
                return trySkipBulkStringAllowNull(in, startIdx);
            }
            case '!':
            case '=': {
                return trySkipBulkStringNoNull(in, startIdx);
            }
            case '*': {
                return trySkipArray(in, startIdx, nestingDepth);
            }
            case '%': {
                return trySkipMap(in, startIdx, nestingDepth);
            }
            case '~': {
                return trySkipSet(in, startIdx, nestingDepth);
            }
            case '>': {
                return trySkipPush(in, startIdx, nestingDepth);
            }
            case '|': {
                int endAttrs = trySkipAttributeMapValue(in, startIdx, nestingDepth);
                if (endAttrs < 0) {
                    return -1;
                }
                int endVal = trySkipOne(in, nestingDepth + 1);
                if (endVal < 0) {
                    in.readerIndex(startIdx);
                    return -1;
                }
                return endVal;
            }
            case '_': {
                if (in.readableBytes() < 2) {
                    in.readerIndex(startIdx);
                    return -1;
                }
                if (in.readByte() != RespDecodingSupport.CR || in.readByte() != RespDecodingSupport.LF) {
                    throw new IllegalArgumentException("Protocol error: bad null CRLF");
                }
                return in.readerIndex();
            }
            default:
                throw new IllegalArgumentException("Protocol error: unknown RESP prefix: " + (char) prefix);
        }
    }

    private int trySkipBoolean(ByteBuf in, int startIdx) {
        if (in.readableBytes() < 3) {
            in.readerIndex(startIdx);
            return -1;
        }
        byte v = in.readByte();
        if (v != 't' && v != 'f') {
            throw new IllegalArgumentException("Protocol error: invalid boolean value");
        }
        if (in.readByte() != RespDecodingSupport.CR || in.readByte() != RespDecodingSupport.LF) {
            throw new IllegalArgumentException("Protocol error: bad boolean CRLF");
        }
        return in.readerIndex();
    }

    private int trySkipLine(ByteBuf in, int startIdx) {
        int lineStart = in.readerIndex();
        int lineEnd = RespDecodingSupport.indexOfCrlf(in, lineStart, maxLineBytes);
        if (lineEnd < 0) {
            if (in.writerIndex() - lineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            in.readerIndex(startIdx);
            return -1;
        }
        int end = lineEnd + 2;
        in.readerIndex(end);
        return end;
    }

    private int trySkipInteger(ByteBuf in, int startIdx) {
        int lineStart = in.readerIndex();
        int lineEnd = RespDecodingSupport.indexOfCrlf(in, lineStart, maxLineBytes);
        if (lineEnd < 0) {
            if (in.writerIndex() - lineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            in.readerIndex(startIdx);
            return -1;
        }

        RespDecodingSupport.parseLongAscii(in, lineStart, lineEnd);

        int end = lineEnd + 2;
        in.readerIndex(end);
        return end;
    }

    private int trySkipBulkStringAllowNull(ByteBuf in, int startIdx) {
        return trySkipBulkString(in, startIdx, true);
    }

    private int trySkipBulkStringNoNull(ByteBuf in, int startIdx) {
        return trySkipBulkString(in, startIdx, false);
    }

    private int trySkipBulkString(ByteBuf in, int startIdx, boolean allowNull) {
        int lenLineStart = in.readerIndex();
        int lenLineEnd = RespDecodingSupport.indexOfCrlf(in, lenLineStart, maxLineBytes);
        if (lenLineEnd < 0) {
            if (in.writerIndex() - lenLineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            in.readerIndex(startIdx);
            return -1;
        }

        if (RespDecodingSupport.isSingleCharLine(in, lenLineStart, lenLineEnd, (byte) '?')) {
            in.readerIndex(lenLineEnd + 2);
            return trySkipStreamedBlobString(in, startIdx);
        }

        int len = RespDecodingSupport.parseIntAscii(in, lenLineStart, lenLineEnd);
        if (len == -1) {
            if (!allowNull) {
                throw new IllegalArgumentException("Protocol error: invalid bulk length");
            }
            int end = lenLineEnd + 2;
            in.readerIndex(end);
            return end;
        }
        if (len < -1) {
            throw new IllegalArgumentException("Protocol error: invalid bulk length");
        }
        if (len > maxBulkBytes) {
            throw new IllegalArgumentException("Protocol error: bulk length too large");
        }

        long dataStart = (long) lenLineEnd + 2;
        long dataEnd = dataStart + (long) len;
        long end = dataEnd + 2;
        if (end > Integer.MAX_VALUE) {
            in.readerIndex(startIdx);
            return -1;
        }
        if (in.writerIndex() < end) {
            in.readerIndex(startIdx);
            return -1;
        }

        int dataEndIdx = (int) dataEnd;
        if (in.getByte(dataEndIdx) != RespDecodingSupport.CR || in.getByte(dataEndIdx + 1) != RespDecodingSupport.LF) {
            throw new IllegalArgumentException("Protocol error: bad bulk string CRLF");
        }
        int endIdx = (int) end;
        in.readerIndex(endIdx);
        return endIdx;
    }

    private int trySkipStreamedBlobString(ByteBuf in, int startIdx) {
        int total = 0;
        for (; ; ) {
            if (!in.isReadable()) {
                in.readerIndex(startIdx);
                return -1;
            }
            if (in.readByte() != ';') {
                throw new IllegalArgumentException("Protocol error: invalid streamed blob chunk prefix");
            }

            int lenLineStart = in.readerIndex();
            int lenLineEnd = RespDecodingSupport.indexOfCrlf(in, lenLineStart, maxLineBytes);
            if (lenLineEnd < 0) {
                if (in.writerIndex() - lenLineStart > maxLineBytes + 2) {
                    throw new IllegalArgumentException("Protocol error: line too long");
                }
                in.readerIndex(startIdx);
                return -1;
            }

            int len;
            try {
                len = RespDecodingSupport.parseIntAscii(in, lenLineStart, lenLineEnd);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Protocol error: invalid streamed blob chunk length");
            }
            if (len < 0) {
                throw new IllegalArgumentException("Protocol error: invalid streamed blob chunk length");
            }
            in.readerIndex(lenLineEnd + 2);
            if (len == 0) {
                return in.readerIndex();
            }
            if (total > maxBulkBytes - len) {
                throw new IllegalArgumentException("Protocol error: bulk length too large");
            }

            long dataStart = (long) in.readerIndex();
            long dataEnd = dataStart + (long) len;
            long end = dataEnd + 2;
            if (end > Integer.MAX_VALUE) {
                in.readerIndex(startIdx);
                return -1;
            }
            if (in.writerIndex() < end) {
                in.readerIndex(startIdx);
                return -1;
            }
            int dataEndIdx = (int) dataEnd;
            if (in.getByte(dataEndIdx) != RespDecodingSupport.CR || in.getByte(dataEndIdx + 1) != RespDecodingSupport.LF) {
                throw new IllegalArgumentException("Protocol error: bad bulk string CRLF");
            }
            in.readerIndex((int) end);
            total += len;
        }
    }

    private int trySkipArray(ByteBuf in, int startIdx, int nestingDepth) {
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested arrays too deep");
        }

        int countLineStart = in.readerIndex();
        int countLineEnd = RespDecodingSupport.indexOfCrlf(in, countLineStart, maxLineBytes);
        if (countLineEnd < 0) {
            if (in.writerIndex() - countLineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            in.readerIndex(startIdx);
            return -1;
        }

        if (RespDecodingSupport.isSingleCharLine(in, countLineStart, countLineEnd, (byte) '?')) {
            in.readerIndex(countLineEnd + 2);
            return trySkipStreamedArray(in, startIdx, nestingDepth);
        }

        int count = RespDecodingSupport.parseIntAscii(in, countLineStart, countLineEnd);
        if (count == -1) {
            int end = countLineEnd + 2;
            in.readerIndex(end);
            return end;
        }
        if (count < -1) {
            throw new IllegalArgumentException("Protocol error: invalid array length");
        }
        if (count > maxArgs) {
            throw new IllegalArgumentException("Protocol error: array length too large");
        }

        in.readerIndex(countLineEnd + 2);
        for (int i = 0; i < count; i++) {
            int endIdx = trySkipOne(in, nestingDepth + 1);
            if (endIdx < 0) {
                in.readerIndex(startIdx);
                return -1;
            }
        }
        return in.readerIndex();
    }

    private int trySkipStreamedArray(ByteBuf in, int startIdx, int nestingDepth) {
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested arrays too deep");
        }

        int count = 0;
        for (; ; ) {
            if (!in.isReadable()) {
                in.readerIndex(startIdx);
                return -1;
            }
            byte next = in.getByte(in.readerIndex());
            if (next == '.') {
                if (in.readableBytes() < 3) {
                    in.readerIndex(startIdx);
                    return -1;
                }
                in.readByte();
                if (in.readByte() != RespDecodingSupport.CR || in.readByte() != RespDecodingSupport.LF) {
                    throw new IllegalArgumentException("Protocol error: bad streamed aggregate end marker");
                }
                return in.readerIndex();
            }

            if (count >= maxArgs) {
                throw new IllegalArgumentException("Protocol error: array length too large");
            }
            int endIdx = trySkipOne(in, nestingDepth + 1);
            if (endIdx < 0) {
                in.readerIndex(startIdx);
                return -1;
            }
            count++;
        }
    }

    private int trySkipMap(ByteBuf in, int startIdx, int nestingDepth) {
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested maps too deep");
        }

        int pairsLineStart = in.readerIndex();
        int pairsLineEnd = RespDecodingSupport.indexOfCrlf(in, pairsLineStart, maxLineBytes);
        if (pairsLineEnd < 0) {
            if (in.writerIndex() - pairsLineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            in.readerIndex(startIdx);
            return -1;
        }

        if (RespDecodingSupport.isSingleCharLine(in, pairsLineStart, pairsLineEnd, (byte) '?')) {
            in.readerIndex(pairsLineEnd + 2);
            return trySkipStreamedMap(in, startIdx, nestingDepth);
        }

        int pairs = RespDecodingSupport.parseIntAscii(in, pairsLineStart, pairsLineEnd);
        if (pairs < 0) {
            throw new IllegalArgumentException("Protocol error: invalid map length");
        }
        if (pairs > maxArgs) {
            throw new IllegalArgumentException("Protocol error: map length too large");
        }

        in.readerIndex(pairsLineEnd + 2);
        for (int i = 0; i < pairs; i++) {
            int endKey = trySkipOne(in, nestingDepth + 1);
            if (endKey < 0) {
                in.readerIndex(startIdx);
                return -1;
            }
            int endVal = trySkipOne(in, nestingDepth + 1);
            if (endVal < 0) {
                in.readerIndex(startIdx);
                return -1;
            }
        }
        return in.readerIndex();
    }

    private int trySkipStreamedMap(ByteBuf in, int startIdx, int nestingDepth) {
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested maps too deep");
        }

        int pairs = 0;
        for (; ; ) {
            if (!in.isReadable()) {
                in.readerIndex(startIdx);
                return -1;
            }
            byte next = in.getByte(in.readerIndex());
            if (next == '.') {
                if (in.readableBytes() < 3) {
                    in.readerIndex(startIdx);
                    return -1;
                }
                in.readByte();
                if (in.readByte() != RespDecodingSupport.CR || in.readByte() != RespDecodingSupport.LF) {
                    throw new IllegalArgumentException("Protocol error: bad streamed aggregate end marker");
                }
                return in.readerIndex();
            }

            if (pairs >= maxArgs) {
                throw new IllegalArgumentException("Protocol error: map length too large");
            }
            int endKey = trySkipOne(in, nestingDepth + 1);
            if (endKey < 0) {
                in.readerIndex(startIdx);
                return -1;
            }

            if (!in.isReadable()) {
                in.readerIndex(startIdx);
                return -1;
            }
            byte maybeEnd = in.getByte(in.readerIndex());
            if (maybeEnd == '.') {
                if (in.readableBytes() < 3) {
                    in.readerIndex(startIdx);
                    return -1;
                }
                throw new IllegalArgumentException("Protocol error: missing map value before end marker");
            }

            int endVal = trySkipOne(in, nestingDepth + 1);
            if (endVal < 0) {
                in.readerIndex(startIdx);
                return -1;
            }
            pairs++;
        }
    }

    private int trySkipSet(ByteBuf in, int startIdx, int nestingDepth) {
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested sets too deep");
        }

        int countLineStart = in.readerIndex();
        int countLineEnd = RespDecodingSupport.indexOfCrlf(in, countLineStart, maxLineBytes);
        if (countLineEnd < 0) {
            if (in.writerIndex() - countLineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            in.readerIndex(startIdx);
            return -1;
        }

        if (RespDecodingSupport.isSingleCharLine(in, countLineStart, countLineEnd, (byte) '?')) {
            in.readerIndex(countLineEnd + 2);
            return trySkipStreamedSet(in, startIdx, nestingDepth);
        }

        int count = RespDecodingSupport.parseIntAscii(in, countLineStart, countLineEnd);
        if (count < 0) {
            throw new IllegalArgumentException("Protocol error: invalid set length");
        }
        if (count > maxArgs) {
            throw new IllegalArgumentException("Protocol error: set length too large");
        }

        in.readerIndex(countLineEnd + 2);
        for (int i = 0; i < count; i++) {
            int endIdx = trySkipOne(in, nestingDepth + 1);
            if (endIdx < 0) {
                in.readerIndex(startIdx);
                return -1;
            }
        }
        return in.readerIndex();
    }

    private int trySkipStreamedSet(ByteBuf in, int startIdx, int nestingDepth) {
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested sets too deep");
        }

        int count = 0;
        for (; ; ) {
            if (!in.isReadable()) {
                in.readerIndex(startIdx);
                return -1;
            }
            byte next = in.getByte(in.readerIndex());
            if (next == '.') {
                if (in.readableBytes() < 3) {
                    in.readerIndex(startIdx);
                    return -1;
                }
                in.readByte();
                if (in.readByte() != RespDecodingSupport.CR || in.readByte() != RespDecodingSupport.LF) {
                    throw new IllegalArgumentException("Protocol error: bad streamed aggregate end marker");
                }
                return in.readerIndex();
            }

            if (count >= maxArgs) {
                throw new IllegalArgumentException("Protocol error: set length too large");
            }
            int endIdx = trySkipOne(in, nestingDepth + 1);
            if (endIdx < 0) {
                in.readerIndex(startIdx);
                return -1;
            }
            count++;
        }
    }

    private int trySkipPush(ByteBuf in, int startIdx, int nestingDepth) {
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested push messages too deep");
        }

        int countLineStart = in.readerIndex();
        int countLineEnd = RespDecodingSupport.indexOfCrlf(in, countLineStart, maxLineBytes);
        if (countLineEnd < 0) {
            if (in.writerIndex() - countLineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            in.readerIndex(startIdx);
            return -1;
        }

        int count = RespDecodingSupport.parseIntAscii(in, countLineStart, countLineEnd);
        if (count < 0) {
            throw new IllegalArgumentException("Protocol error: invalid push length");
        }
        if (count > maxArgs) {
            throw new IllegalArgumentException("Protocol error: push length too large");
        }

        in.readerIndex(countLineEnd + 2);
        for (int i = 0; i < count; i++) {
            int endIdx = trySkipOne(in, nestingDepth + 1);
            if (endIdx < 0) {
                in.readerIndex(startIdx);
                return -1;
            }
        }
        return in.readerIndex();
    }

    private int trySkipAttributeMapValue(ByteBuf in, int startIdx, int nestingDepth) {
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested attributes too deep");
        }

        int pairsLineStart = in.readerIndex();
        int pairsLineEnd = RespDecodingSupport.indexOfCrlf(in, pairsLineStart, maxLineBytes);
        if (pairsLineEnd < 0) {
            if (in.writerIndex() - pairsLineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            in.readerIndex(startIdx);
            return -1;
        }

        int pairs = RespDecodingSupport.parseIntAscii(in, pairsLineStart, pairsLineEnd);
        if (pairs < 0) {
            throw new IllegalArgumentException("Protocol error: invalid attribute length");
        }
        if (pairs > maxArgs) {
            throw new IllegalArgumentException("Protocol error: attribute length too large");
        }

        in.readerIndex(pairsLineEnd + 2);
        for (int i = 0; i < pairs; i++) {
            int endKey = trySkipOne(in, nestingDepth + 1);
            if (endKey < 0) {
                in.readerIndex(startIdx);
                return -1;
            }
            int endVal = trySkipOne(in, nestingDepth + 1);
            if (endVal < 0) {
                in.readerIndex(startIdx);
                return -1;
            }
        }
        return in.readerIndex();
    }

    private boolean tryDecodeBulkStringArg(ByteBuf in, int frameStartIdx, RespCommand cmd, int argIndex) {
        int startIdx = in.readerIndex() - 1;

        int lenLineStart = in.readerIndex();
        int lenLineEnd = RespDecodingSupport.indexOfCrlf(in, lenLineStart, maxLineBytes);
        if (lenLineEnd < 0) {
            if (in.writerIndex() - lenLineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            in.readerIndex(startIdx);
            return false;
        }

        if (RespDecodingSupport.isSingleCharLine(in, lenLineStart, lenLineEnd, (byte) '?')) {
            // Streamed blob string is not contiguous; fall back to the materialized decode path.
            throw MATERIALIZE;
        }

        int len = RespDecodingSupport.parseIntAscii(in, lenLineStart, lenLineEnd);
        if (len == -1) {
            in.readerIndex(lenLineEnd + 2);
            RespCommandBuilder.setArgNull(cmd, argIndex);
            return true;
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
            in.readerIndex(startIdx);
            return false;
        }

        if (in.getByte(dataEnd) != RespDecodingSupport.CR || in.getByte(dataEnd + 1) != RespDecodingSupport.LF) {
            throw new IllegalArgumentException("Protocol error: bad bulk string CRLF");
        }

        RespCommandBuilder.setArgSlice(cmd, argIndex, dataStart - frameStartIdx, len);
        in.readerIndex(end);
        return true;
    }

    private boolean tryDecodeBlobErrorArg(ByteBuf in, int frameStartIdx, RespCommand cmd, int argIndex) {
        int startIdx = in.readerIndex() - 1;

        int lenLineStart = in.readerIndex();
        int lenLineEnd = RespDecodingSupport.indexOfCrlf(in, lenLineStart, maxLineBytes);
        if (lenLineEnd < 0) {
            if (in.writerIndex() - lenLineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            in.readerIndex(startIdx);
            return false;
        }

        if (RespDecodingSupport.isSingleCharLine(in, lenLineStart, lenLineEnd, (byte) '?')) {
            throw new IllegalArgumentException("Protocol error: invalid blob error length");
        }

        int len = RespDecodingSupport.parseIntAscii(in, lenLineStart, lenLineEnd);
        if (len < 0) {
            throw new IllegalArgumentException("Protocol error: invalid blob error length");
        }
        if (len > maxBulkBytes) {
            throw new IllegalArgumentException("Protocol error: bulk length too large");
        }

        int dataStart = lenLineEnd + 2;
        int dataEnd = dataStart + len;
        int end = dataEnd + 2;
        if (in.writerIndex() < end) {
            in.readerIndex(startIdx);
            return false;
        }

        if (in.getByte(dataEnd) != RespDecodingSupport.CR || in.getByte(dataEnd + 1) != RespDecodingSupport.LF) {
            throw new IllegalArgumentException("Protocol error: bad bulk string CRLF");
        }

        RespCommandBuilder.setArgSlice(cmd, argIndex, dataStart - frameStartIdx, len);
        in.readerIndex(end);
        return true;
    }

    private boolean tryDecodeNullArg(ByteBuf in, RespCommand cmd, int argIndex) {
        int startIdx = in.readerIndex() - 1;
        if (in.readableBytes() < 2) {
            in.readerIndex(startIdx);
            return false;
        }
        if (in.readByte() != RespDecodingSupport.CR || in.readByte() != RespDecodingSupport.LF) {
            throw new IllegalArgumentException("Protocol error: bad null CRLF");
        }
        RespCommandBuilder.setArgNull(cmd, argIndex);
        return true;
    }

    private boolean tryDecodeLineArg(ByteBuf in, int frameStartIdx, RespCommand cmd, int argIndex) {
        int startIdx = in.readerIndex() - 1;

        int lineStart = in.readerIndex();
        int lineEnd = RespDecodingSupport.indexOfCrlf(in, lineStart, maxLineBytes);
        if (lineEnd < 0) {
            if (in.writerIndex() - lineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            in.readerIndex(startIdx);
            return false;
        }

        RespCommandBuilder.setArgSlice(cmd, argIndex, lineStart - frameStartIdx, lineEnd - lineStart);
        in.readerIndex(lineEnd + 2);
        return true;
    }

    private boolean tryDecodeBooleanArg(ByteBuf in, int frameStartIdx, RespCommand cmd, int argIndex) {
        int startIdx = in.readerIndex() - 1;
        if (in.readableBytes() < 3) {
            in.readerIndex(startIdx);
            return false;
        }
        byte v = in.readByte();
        if (v != 't' && v != 'f') {
            throw new IllegalArgumentException("Protocol error: invalid boolean value");
        }
        if (in.readByte() != RespDecodingSupport.CR || in.readByte() != RespDecodingSupport.LF) {
            throw new IllegalArgumentException("Protocol error: bad boolean CRLF");
        }
        RespCommandBuilder.setArgSlice(cmd, argIndex, (in.readerIndex() - 3) - frameStartIdx, 1);
        return true;
    }

    private boolean tryDecodeVerbatimStringArg(ByteBuf in, int frameStartIdx, RespCommand cmd, int argIndex) {
        int startIdx = in.readerIndex() - 1;

        int lenLineStart = in.readerIndex();
        int lenLineEnd = RespDecodingSupport.indexOfCrlf(in, lenLineStart, maxLineBytes);
        if (lenLineEnd < 0) {
            if (in.writerIndex() - lenLineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            in.readerIndex(startIdx);
            return false;
        }

        int len = RespDecodingSupport.parseIntAscii(in, lenLineStart, lenLineEnd);
        if (len < 0) {
            throw new IllegalArgumentException("Protocol error: invalid verbatim string length");
        }
        if (len > maxBulkBytes) {
            throw new IllegalArgumentException("Protocol error: bulk length too large");
        }

        int payloadStart = lenLineEnd + 2;
        int payloadEnd = payloadStart + len;
        int end = payloadEnd + 2;
        if (in.writerIndex() < end) {
            in.readerIndex(startIdx);
            return false;
        }

        if (len < 4 || in.getByte(payloadStart + 3) != (byte) ':') {
            throw new IllegalArgumentException("Protocol error: invalid verbatim string payload");
        }
        if (in.getByte(payloadEnd) != RespDecodingSupport.CR || in.getByte(payloadEnd + 1) != RespDecodingSupport.LF) {
            throw new IllegalArgumentException("Protocol error: bad bulk string CRLF");
        }

        // 仅取 "fmt:" 之后的内容作为参数，忽略 3-char format。
        RespCommandBuilder.setArgSlice(cmd, argIndex, (payloadStart + 4) - frameStartIdx, len - 4);
        in.readerIndex(end);
        return true;
    }

    private RespCommand decodeInlineCommand(ByteBuf in, int startIdx) {
        int lineEnd = RespDecodingSupport.indexOfCrlf(in, startIdx, maxLineBytes);
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

    // 兼容旧代码：将参数校验逻辑委托到公共工具类，避免 request/response 双轨漂移。
    private static int requirePositive(int value, String name) {
        return RespDecodingSupport.requirePositive(value, name);
    }
}
