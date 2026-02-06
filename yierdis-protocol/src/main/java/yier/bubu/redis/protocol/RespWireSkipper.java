package yier.bubu.redis.protocol;

// RESP wire 层的 skip/scan 逻辑（Netty-free SSOT）：用于 framing/decoder 在不构建对象树的前提下定位一个完整对象的边界。

import yier.bubu.redis.bytes.BytesSource;

/**
 * RESP wire skipper：在不分配对象的前提下“跳过”一个完整 RESP 值，并返回 endIndex（exclusive）。
 * <p>
 * 约定：
 * - 返回 -1 表示数据不足（需要更多 bytes）
 * - 其它返回值为 endIndex（exclusive），且必须大于 startIndex
 * - 协议错误抛出 {@link IllegalArgumentException}，message 以 "Protocol error:" 开头
 */
public final class RespWireSkipper {
    private RespWireSkipper() {
    }

    /**
     * reply/framing 场景：与历史实现保持一致（对 {@code !}/{@code =} 采用“宽松 bulk”语义：允许 -1）。
     */
    public static int trySkipOne(BytesSource in,
                                 int startIndex,
                                 int limit,
                                 int maxBulkBytes,
                                 int maxArrayLen,
                                 int maxNestingDepth,
                                 int maxLineBytes) {
        return trySkipOne0(in, startIndex, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, 0, true);
    }

    /**
     * request/strict 场景：对 {@code !}/{@code =} 采用严格 bulk 语义（禁止 -1），用于跳过 attributes map 内部值等。
     */
    public static int trySkipOneStrict(BytesSource in,
                                       int startIndex,
                                       int limit,
                                       int maxBulkBytes,
                                       int maxArrayLen,
                                       int maxNestingDepth,
                                       int maxLineBytes) {
        return trySkipOne0(in, startIndex, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, 0, false);
    }

    /**
     * 跳过一个 attribute map（仅 map 本身，不包含其后的 value）。
     * <p>
     * 该方法用于 request decoder 在命令前忽略 attributes metadata（支持链式 attributes）。
     */
    public static int trySkipAttributeMapOnly(BytesSource in,
                                              int startIndex,
                                              int limit,
                                              int maxBulkBytes,
                                              int maxArrayLen,
                                              int maxNestingDepth,
                                              int maxLineBytes) {
        if (startIndex >= limit) {
            return -1;
        }
        if (in.getByte(startIndex) != '|') {
            throw new IllegalStateException("expected attribute prefix");
        }
        int endMap = trySkipAttributeMapBody(in, startIndex, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, 0, false, false);
        if (endMap < 0) {
            return -1;
        }
        return endMap;
    }

    private static int trySkipOne0(BytesSource in,
                                   int startIndex,
                                   int limit,
                                   int maxBulkBytes,
                                   int maxArrayLen,
                                   int maxNestingDepth,
                                   int maxLineBytes,
                                   int nestingDepth,
                                   boolean allowNullForNonDollarBulk) {
        if (in == null) {
            throw new IllegalArgumentException("in must not be null");
        }
        RespWireSupport.requirePositive(maxBulkBytes, "maxBulkBytes");
        RespWireSupport.requirePositive(maxArrayLen, "maxArrayLen");
        RespWireSupport.requirePositive(maxNestingDepth, "maxNestingDepth");
        RespWireSupport.requirePositive(maxLineBytes, "maxLineBytes");
        if (startIndex < 0 || limit < 0 || startIndex > limit) {
            throw new IllegalArgumentException("invalid range: startIndex=" + startIndex + ", limit=" + limit);
        }
        if (startIndex >= limit) {
            return -1;
        }

        byte prefix = in.getByte(startIndex);
        switch (prefix) {
            case '+':
            case '-':
            case ',':
            case '(': {
                return trySkipLine(in, startIndex + 1, limit, maxLineBytes);
            }
            case ':': {
                return trySkipInteger(in, startIndex + 1, limit, maxLineBytes);
            }
            case '#': {
                return trySkipBoolean(in, startIndex, limit);
            }
            case '$': {
                return trySkipBulkString(in, startIndex, limit, maxBulkBytes, maxLineBytes, true);
            }
            case '!':
            case '=': {
                return trySkipBulkString(in, startIndex, limit, maxBulkBytes, maxLineBytes, allowNullForNonDollarBulk);
            }
            case '*': {
                return trySkipArray(in, startIndex, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, nestingDepth, allowNullForNonDollarBulk);
            }
            case '%': {
                return trySkipMap(in, startIndex, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, nestingDepth, allowNullForNonDollarBulk);
            }
            case '~': {
                return trySkipSet(in, startIndex, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, nestingDepth, allowNullForNonDollarBulk);
            }
            case '>': {
                return trySkipPush(in, startIndex, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, nestingDepth, allowNullForNonDollarBulk);
            }
            case '|': {
                int endMap = trySkipAttributeMapBody(in, startIndex, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, nestingDepth, allowNullForNonDollarBulk, true);
                if (endMap < 0) {
                    return -1;
                }
                int endVal = trySkipOne0(in, endMap, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, nestingDepth + 1, allowNullForNonDollarBulk);
                if (endVal < 0) {
                    return -1;
                }
                return endVal;
            }
            case '_': {
                return trySkipNull(in, startIndex, limit);
            }
            default:
                throw new IllegalArgumentException("Protocol error: unknown RESP prefix: " + (char) prefix);
        }
    }

    private static int trySkipNull(BytesSource in, int startIndex, int limit) {
        // "_\r\n"
        if (limit - startIndex < 3) {
            return -1;
        }
        if (in.getByte(startIndex + 1) != RespWireSupport.CR || in.getByte(startIndex + 2) != RespWireSupport.LF) {
            throw new IllegalArgumentException("Protocol error: bad null CRLF");
        }
        return startIndex + 3;
    }

    private static int trySkipBoolean(BytesSource in, int startIndex, int limit) {
        // "#t\r\n" / "#f\r\n"
        if (limit - startIndex < 4) {
            return -1;
        }
        byte v = in.getByte(startIndex + 1);
        if (v != 't' && v != 'f') {
            throw new IllegalArgumentException("Protocol error: invalid boolean value");
        }
        if (in.getByte(startIndex + 2) != RespWireSupport.CR || in.getByte(startIndex + 3) != RespWireSupport.LF) {
            throw new IllegalArgumentException("Protocol error: bad boolean CRLF");
        }
        return startIndex + 4;
    }

    private static int trySkipLine(BytesSource in, int lineStart, int limit, int maxLineBytes) {
        int lineEnd = RespWireSupport.indexOfCrlf(in, lineStart, limit, maxLineBytes);
        if (lineEnd < 0) {
            if (limit - lineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            return -1;
        }
        return lineEnd + 2;
    }

    private static int trySkipInteger(BytesSource in, int lineStart, int limit, int maxLineBytes) {
        int lineEnd = RespWireSupport.indexOfCrlf(in, lineStart, limit, maxLineBytes);
        if (lineEnd < 0) {
            if (limit - lineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            return -1;
        }
        RespWireSupport.parseLongAscii(in, lineStart, lineEnd);
        return lineEnd + 2;
    }

    private static int trySkipBulkString(BytesSource in,
                                         int startIndex,
                                         int limit,
                                         int maxBulkBytes,
                                         int maxLineBytes,
                                         boolean allowNull) {
        int lenLineStart = startIndex + 1;
        int lenLineEnd = RespWireSupport.indexOfCrlf(in, lenLineStart, limit, maxLineBytes);
        if (lenLineEnd < 0) {
            if (limit - lenLineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            return -1;
        }

        if (RespWireSupport.isSingleCharLine(in, lenLineStart, lenLineEnd, (byte) '?')) {
            int afterHeader = lenLineEnd + 2;
            return trySkipStreamedBlobString(in, afterHeader, limit, maxBulkBytes, maxLineBytes);
        }

        int len = RespWireSupport.parseIntAscii(in, lenLineStart, lenLineEnd);
        if (len == -1) {
            if (!allowNull) {
                throw new IllegalArgumentException("Protocol error: invalid bulk length");
            }
            return lenLineEnd + 2;
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
            return -1;
        }
        if (end > (long) limit) {
            return -1;
        }

        int dataEndIdx = (int) dataEnd;
        if (in.getByte(dataEndIdx) != RespWireSupport.CR || in.getByte(dataEndIdx + 1) != RespWireSupport.LF) {
            throw new IllegalArgumentException("Protocol error: bad bulk string CRLF");
        }
        return (int) end;
    }

    private static int trySkipStreamedBlobString(BytesSource in,
                                                 int index,
                                                 int limit,
                                                 int maxBulkBytes,
                                                 int maxLineBytes) {
        // "$?\r\n" + ( ";"<len>\r\n<payload>\r\n )* + ";0\r\n"
        int total = 0;
        int idx = index;
        for (; ; ) {
            if (idx >= limit) {
                return -1;
            }
            byte chunkPrefix = in.getByte(idx++);
            if (chunkPrefix != ';') {
                throw new IllegalArgumentException("Protocol error: invalid streamed blob chunk prefix");
            }

            int lenLineStart = idx;
            int lenLineEnd = RespWireSupport.indexOfCrlf(in, lenLineStart, limit, maxLineBytes);
            if (lenLineEnd < 0) {
                if (limit - lenLineStart > maxLineBytes + 2) {
                    throw new IllegalArgumentException("Protocol error: line too long");
                }
                return -1;
            }

            int len;
            try {
                len = RespWireSupport.parseIntAscii(in, lenLineStart, lenLineEnd);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Protocol error: invalid streamed blob chunk length");
            }
            if (len < 0) {
                throw new IllegalArgumentException("Protocol error: invalid streamed blob chunk length");
            }

            idx = lenLineEnd + 2;
            if (len == 0) {
                return idx;
            }
            if (total > maxBulkBytes - len) {
                throw new IllegalArgumentException("Protocol error: bulk length too large");
            }

            long dataStart = (long) idx;
            long dataEnd = dataStart + (long) len;
            long end = dataEnd + 2;
            if (end > Integer.MAX_VALUE) {
                return -1;
            }
            if (end > (long) limit) {
                return -1;
            }

            int dataEndIdx = (int) dataEnd;
            if (in.getByte(dataEndIdx) != RespWireSupport.CR || in.getByte(dataEndIdx + 1) != RespWireSupport.LF) {
                throw new IllegalArgumentException("Protocol error: bad bulk string CRLF");
            }

            idx = (int) end;
            total += len;
        }
    }

    private static int trySkipArray(BytesSource in,
                                    int startIndex,
                                    int limit,
                                    int maxBulkBytes,
                                    int maxArrayLen,
                                    int maxNestingDepth,
                                    int maxLineBytes,
                                    int nestingDepth,
                                    boolean allowNullForNonDollarBulk) {
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested arrays too deep");
        }

        int countLineStart = startIndex + 1;
        int countLineEnd = RespWireSupport.indexOfCrlf(in, countLineStart, limit, maxLineBytes);
        if (countLineEnd < 0) {
            if (limit - countLineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            return -1;
        }

        if (RespWireSupport.isSingleCharLine(in, countLineStart, countLineEnd, (byte) '?')) {
            int afterHeader = countLineEnd + 2;
            return trySkipStreamedArray(in, afterHeader, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, nestingDepth, allowNullForNonDollarBulk);
        }

        int count = RespWireSupport.parseIntAscii(in, countLineStart, countLineEnd);
        if (count == -1) {
            return countLineEnd + 2;
        }
        if (count < -1) {
            throw new IllegalArgumentException("Protocol error: invalid array length");
        }
        if (count > maxArrayLen) {
            throw new IllegalArgumentException("Protocol error: array length too large");
        }

        int idx = countLineEnd + 2;
        for (int i = 0; i < count; i++) {
            int end = trySkipOne0(in, idx, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, nestingDepth + 1, allowNullForNonDollarBulk);
            if (end < 0) {
                return -1;
            }
            idx = end;
        }
        return idx;
    }

    private static int trySkipStreamedArray(BytesSource in,
                                           int index,
                                           int limit,
                                           int maxBulkBytes,
                                           int maxArrayLen,
                                           int maxNestingDepth,
                                           int maxLineBytes,
                                           int nestingDepth,
                                           boolean allowNullForNonDollarBulk) {
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested arrays too deep");
        }

        int count = 0;
        int idx = index;
        for (; ; ) {
            if (idx >= limit) {
                return -1;
            }
            byte next = in.getByte(idx);
            if (next == '.') {
                if (limit - idx < 3) {
                    return -1;
                }
                if (in.getByte(idx + 1) != RespWireSupport.CR || in.getByte(idx + 2) != RespWireSupport.LF) {
                    throw new IllegalArgumentException("Protocol error: bad streamed aggregate end marker");
                }
                return idx + 3;
            }

            if (count >= maxArrayLen) {
                throw new IllegalArgumentException("Protocol error: array length too large");
            }
            int end = trySkipOne0(in, idx, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, nestingDepth + 1, allowNullForNonDollarBulk);
            if (end < 0) {
                return -1;
            }
            idx = end;
            count++;
        }
    }

    private static int trySkipMap(BytesSource in,
                                  int startIndex,
                                  int limit,
                                  int maxBulkBytes,
                                  int maxArrayLen,
                                  int maxNestingDepth,
                                  int maxLineBytes,
                                  int nestingDepth,
                                  boolean allowNullForNonDollarBulk) {
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested maps too deep");
        }

        int pairsLineStart = startIndex + 1;
        int pairsLineEnd = RespWireSupport.indexOfCrlf(in, pairsLineStart, limit, maxLineBytes);
        if (pairsLineEnd < 0) {
            if (limit - pairsLineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            return -1;
        }

        if (RespWireSupport.isSingleCharLine(in, pairsLineStart, pairsLineEnd, (byte) '?')) {
            int afterHeader = pairsLineEnd + 2;
            return trySkipStreamedMap(in, afterHeader, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, nestingDepth, allowNullForNonDollarBulk);
        }

        int pairs = RespWireSupport.parseIntAscii(in, pairsLineStart, pairsLineEnd);
        if (pairs < 0) {
            throw new IllegalArgumentException("Protocol error: invalid map length");
        }
        if (pairs > maxArrayLen) {
            throw new IllegalArgumentException("Protocol error: map length too large");
        }

        int idx = pairsLineEnd + 2;
        for (int i = 0; i < pairs; i++) {
            int endKey = trySkipOne0(in, idx, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, nestingDepth + 1, allowNullForNonDollarBulk);
            if (endKey < 0) {
                return -1;
            }
            int endVal = trySkipOne0(in, endKey, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, nestingDepth + 1, allowNullForNonDollarBulk);
            if (endVal < 0) {
                return -1;
            }
            idx = endVal;
        }
        return idx;
    }

    private static int trySkipStreamedMap(BytesSource in,
                                         int index,
                                         int limit,
                                         int maxBulkBytes,
                                         int maxArrayLen,
                                         int maxNestingDepth,
                                         int maxLineBytes,
                                         int nestingDepth,
                                         boolean allowNullForNonDollarBulk) {
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested maps too deep");
        }

        int pairs = 0;
        int idx = index;
        for (; ; ) {
            if (idx >= limit) {
                return -1;
            }
            byte next = in.getByte(idx);
            if (next == '.') {
                if (limit - idx < 3) {
                    return -1;
                }
                if (in.getByte(idx + 1) != RespWireSupport.CR || in.getByte(idx + 2) != RespWireSupport.LF) {
                    throw new IllegalArgumentException("Protocol error: bad streamed aggregate end marker");
                }
                return idx + 3;
            }

            if (pairs >= maxArrayLen) {
                throw new IllegalArgumentException("Protocol error: map length too large");
            }
            int endKey = trySkipOne0(in, idx, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, nestingDepth + 1, allowNullForNonDollarBulk);
            if (endKey < 0) {
                return -1;
            }
            idx = endKey;

            if (idx >= limit) {
                return -1;
            }
            byte maybeEnd = in.getByte(idx);
            if (maybeEnd == '.') {
                if (limit - idx < 3) {
                    return -1;
                }
                throw new IllegalArgumentException("Protocol error: missing map value before end marker");
            }

            int endVal = trySkipOne0(in, idx, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, nestingDepth + 1, allowNullForNonDollarBulk);
            if (endVal < 0) {
                return -1;
            }
            idx = endVal;
            pairs++;
        }
    }

    private static int trySkipSet(BytesSource in,
                                  int startIndex,
                                  int limit,
                                  int maxBulkBytes,
                                  int maxArrayLen,
                                  int maxNestingDepth,
                                  int maxLineBytes,
                                  int nestingDepth,
                                  boolean allowNullForNonDollarBulk) {
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested sets too deep");
        }

        int countLineStart = startIndex + 1;
        int countLineEnd = RespWireSupport.indexOfCrlf(in, countLineStart, limit, maxLineBytes);
        if (countLineEnd < 0) {
            if (limit - countLineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            return -1;
        }

        if (RespWireSupport.isSingleCharLine(in, countLineStart, countLineEnd, (byte) '?')) {
            int afterHeader = countLineEnd + 2;
            return trySkipStreamedSet(in, afterHeader, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, nestingDepth, allowNullForNonDollarBulk);
        }

        int count = RespWireSupport.parseIntAscii(in, countLineStart, countLineEnd);
        if (count < 0) {
            throw new IllegalArgumentException("Protocol error: invalid set length");
        }
        if (count > maxArrayLen) {
            throw new IllegalArgumentException("Protocol error: set length too large");
        }

        int idx = countLineEnd + 2;
        for (int i = 0; i < count; i++) {
            int end = trySkipOne0(in, idx, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, nestingDepth + 1, allowNullForNonDollarBulk);
            if (end < 0) {
                return -1;
            }
            idx = end;
        }
        return idx;
    }

    private static int trySkipStreamedSet(BytesSource in,
                                         int index,
                                         int limit,
                                         int maxBulkBytes,
                                         int maxArrayLen,
                                         int maxNestingDepth,
                                         int maxLineBytes,
                                         int nestingDepth,
                                         boolean allowNullForNonDollarBulk) {
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested sets too deep");
        }

        int count = 0;
        int idx = index;
        for (; ; ) {
            if (idx >= limit) {
                return -1;
            }
            byte next = in.getByte(idx);
            if (next == '.') {
                if (limit - idx < 3) {
                    return -1;
                }
                if (in.getByte(idx + 1) != RespWireSupport.CR || in.getByte(idx + 2) != RespWireSupport.LF) {
                    throw new IllegalArgumentException("Protocol error: bad streamed aggregate end marker");
                }
                return idx + 3;
            }

            if (count >= maxArrayLen) {
                throw new IllegalArgumentException("Protocol error: set length too large");
            }
            int end = trySkipOne0(in, idx, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, nestingDepth + 1, allowNullForNonDollarBulk);
            if (end < 0) {
                return -1;
            }
            idx = end;
            count++;
        }
    }

    private static int trySkipPush(BytesSource in,
                                   int startIndex,
                                   int limit,
                                   int maxBulkBytes,
                                   int maxArrayLen,
                                   int maxNestingDepth,
                                   int maxLineBytes,
                                   int nestingDepth,
                                   boolean allowNullForNonDollarBulk) {
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested push messages too deep");
        }

        int countLineStart = startIndex + 1;
        int countLineEnd = RespWireSupport.indexOfCrlf(in, countLineStart, limit, maxLineBytes);
        if (countLineEnd < 0) {
            if (limit - countLineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            return -1;
        }

        int count = RespWireSupport.parseIntAscii(in, countLineStart, countLineEnd);
        if (count < 0) {
            throw new IllegalArgumentException("Protocol error: invalid push length");
        }
        if (count > maxArrayLen) {
            throw new IllegalArgumentException("Protocol error: push length too large");
        }

        int idx = countLineEnd + 2;
        for (int i = 0; i < count; i++) {
            int end = trySkipOne0(in, idx, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, nestingDepth + 1, allowNullForNonDollarBulk);
            if (end < 0) {
                return -1;
            }
            idx = end;
        }
        return idx;
    }

    private static int trySkipAttributeMapBody(BytesSource in,
                                               int startIndex,
                                               int limit,
                                               int maxBulkBytes,
                                               int maxArrayLen,
                                               int maxNestingDepth,
                                               int maxLineBytes,
                                               int nestingDepth,
                                               boolean allowNullForNonDollarBulk,
                                               boolean wrapped) {
        if (nestingDepth >= maxNestingDepth) {
            throw new IllegalArgumentException("Protocol error: nested attributes too deep");
        }
        int pairsLineStart = startIndex + 1;
        int pairsLineEnd = RespWireSupport.indexOfCrlf(in, pairsLineStart, limit, maxLineBytes);
        if (pairsLineEnd < 0) {
            if (limit - pairsLineStart > maxLineBytes + 2) {
                throw new IllegalArgumentException("Protocol error: line too long");
            }
            return -1;
        }

        int pairs = RespWireSupport.parseIntAscii(in, pairsLineStart, pairsLineEnd);
        if (pairs < 0) {
            throw new IllegalArgumentException("Protocol error: invalid attribute length");
        }
        if (pairs > maxArrayLen) {
            throw new IllegalArgumentException("Protocol error: attribute length too large");
        }

        int idx = pairsLineEnd + 2;
        for (int i = 0; i < pairs; i++) {
            int depthForKeyVal = wrapped ? nestingDepth + 1 : 0;
            int endKey = trySkipOne0(in, idx, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, depthForKeyVal, allowNullForNonDollarBulk);
            if (endKey < 0) {
                return -1;
            }
            int endVal = trySkipOne0(in, endKey, limit, maxBulkBytes, maxArrayLen, maxNestingDepth, maxLineBytes, depthForKeyVal, allowNullForNonDollarBulk);
            if (endVal < 0) {
                return -1;
            }
            idx = endVal;
        }
        return idx;
    }
}

