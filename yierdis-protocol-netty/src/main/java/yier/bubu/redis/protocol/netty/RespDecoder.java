package yier.bubu.redis.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.ByteToMessageDecoder;
import yier.bubu.redis.protocol.RespLimits;

import java.util.List;

/**
 * RESP reply decoder（frame/zero-copy 取向）。
 * <p>
 * 该 decoder 只负责“切帧”：从 ByteBuf 中定位一个完整的 RESP reply，并输出 {@link NettyRespFrame}。
 * 语义解析（例如将 reply 转成 String/long/对象树）由上层按需完成，以减少分配与拷贝。
 */
public final class RespDecoder extends ByteToMessageDecoder {
    private final int maxBulkBytes;
    private final int maxArrayLen;
    private final int maxNestingDepth;
    private final int maxLineBytes;

    public RespDecoder() {
        this(RespLimits.DEFAULT_MAX_BULK_BYTES, RespLimits.DEFAULT_MAX_ARRAY_LEN, RespLimits.DEFAULT_MAX_NESTING_DEPTH, RespLimits.DEFAULT_MAX_LINE_BYTES);
    }

    public RespDecoder(int maxBulkBytes, int maxArrayLen, int maxNestingDepth, int maxLineBytes) {
        this.maxBulkBytes = RespDecodingSupport.requirePositive(maxBulkBytes, "maxBulkBytes");
        this.maxArrayLen = RespDecodingSupport.requirePositive(maxArrayLen, "maxArrayLen");
        this.maxNestingDepth = RespDecodingSupport.requirePositive(maxNestingDepth, "maxNestingDepth");
        this.maxLineBytes = RespDecodingSupport.requirePositive(maxLineBytes, "maxLineBytes");
    }

    @Override
    protected void decode(io.netty.channel.ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        for (; ; ) {
            int startIdx = in.readerIndex();
            int endIdx = trySkipOne(in, 0);
            if (endIdx < 0) {
                in.readerIndex(startIdx);
                return;
            }

            int len = endIdx - startIdx;
            ByteBuf frameBuf = in.retainedSlice(startIdx, len);
            out.add(new NettyRespFrame(frameBuf));
        }
    }

    /**
     * 尝试跳过一个完整 reply。
     *
     * @return reply 的 endIndex（absolute index），若数据不足则返回 -1（并将 readerIndex 回滚到 start）
     */
    private int trySkipOne(ByteBuf in, int nestingDepth) {
        int startIdx = in.readerIndex();
        if (!in.isReadable()) {
            return -1;
        }

        byte prefix = in.readByte();
        switch (prefix) {
            case '+':
            case '-': {
                return trySkipLine(in, startIdx);
            }
            case ':': {
                return trySkipInteger(in, startIdx);
            }
            case '#': {
                return trySkipBoolean(in, startIdx);
            }
            case ',': {
                return trySkipLine(in, startIdx);
            }
            case '(': {
                return trySkipLine(in, startIdx);
            }
            case '$': {
                return trySkipBulkString(in, startIdx);
            }
            case '!': {
                return trySkipBulkString(in, startIdx);
            }
            case '=': {
                return trySkipBulkString(in, startIdx);
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
                // RESP3 attribute: skip the attributes map AND the following reply as one logical frame.
                int endAttrs = trySkipAttributeMap(in, startIdx, nestingDepth);
                if (endAttrs < 0) {
                    return -1;
                }
                int endValue = trySkipOne(in, nestingDepth + 1);
                if (endValue < 0) {
                    in.readerIndex(startIdx);
                    return -1;
                }
                return endValue;
            }
            case '_': {
                // RESP3 null: "_\r\n"
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

        // 校验语义：整数必须是合法的 long（保持与 server/client 的协议一致性）。
        RespDecodingSupport.parseLongAscii(in, lineStart, lineEnd);

        int end = lineEnd + 2;
        in.readerIndex(end);
        return end;
    }

    private int trySkipBulkString(ByteBuf in, int startIdx) {
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
        // Streamed blob string: "$?\r\n" + ( ";"<len>\r\n<payload>\r\n )* + ";0\r\n"
        int total = 0;
        for (; ; ) {
            if (!in.isReadable()) {
                in.readerIndex(startIdx);
                return -1;
            }
            byte chunkPrefix = in.readByte();
            if (chunkPrefix != ';') {
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
            int endIdx = (int) end;
            in.readerIndex(endIdx);
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
        if (count > maxArrayLen) {
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

            if (count >= maxArrayLen) {
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
        if (pairs > maxArrayLen) {
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

            if (pairs >= maxArrayLen) {
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
        if (count > maxArrayLen) {
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

            if (count >= maxArrayLen) {
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
        if (count > maxArrayLen) {
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

    private int trySkipAttributeMap(ByteBuf in, int startIdx, int nestingDepth) {
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
        if (pairs > maxArrayLen) {
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
}
