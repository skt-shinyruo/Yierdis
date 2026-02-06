package yier.bubu.redis.protocol;

// RESP wire 层的通用解析工具（Netty-free SSOT）：提供 CRLF 扫描与 ASCII 数字解析，供 skipper/parser/adapter 复用。

import yier.bubu.redis.bytes.BytesSource;

final class RespWireSupport {
    static final byte CR = '\r';
    static final byte LF = '\n';

    private RespWireSupport() {
    }

    static int requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be > 0");
        }
        return value;
    }

    /**
     * 在 [start, limit) 区间内查找 CRLF 的 CR 位置（返回 CR 的 index）。
     * <p>
     * - 若未找到，返回 -1（可能是数据不足，也可能是 line 太长；由上层根据 available bytes 判断）。
     */
    static int indexOfCrlf(BytesSource in, int start, int limit, int maxLineBytes) {
        if (in == null) {
            throw new IllegalArgumentException("in must not be null");
        }
        if (start < 0 || limit < 0 || start > limit) {
            throw new IllegalArgumentException("invalid range: start=" + start + ", limit=" + limit);
        }
        if (maxLineBytes <= 0) {
            throw new IllegalArgumentException("maxLineBytes must be > 0");
        }
        if (limit - start < 2) {
            return -1;
        }

        int maxCrlfStart = start + maxLineBytes;
        int scanLimit = Math.min(limit - 1, maxCrlfStart + 1);
        for (int i = start; i < scanLimit; i++) {
            if (in.getByte(i) == CR && in.getByte(i + 1) == LF) {
                return i;
            }
        }
        return -1;
    }

    static boolean isSingleCharLine(BytesSource in, int start, int end, byte expected) {
        int i = start;
        while (i < end && isSpace(in.getByte(i))) {
            i++;
        }
        int j = end;
        while (j > i && isSpace(in.getByte(j - 1))) {
            j--;
        }
        return (j - i) == 1 && in.getByte(i) == expected;
    }

    static int parseIntAscii(BytesSource in, int start, int end) {
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

    static long parseLongAscii(BytesSource in, int start, int end) {
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

        long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        long multMin = limit / 10;
        long result = 0;

        while (i < j) {
            int digit = (in.getByte(i++) & 0xFF) - '0';
            if (digit < 0 || digit > 9) {
                throw new IllegalArgumentException("Protocol error: invalid integer line");
            }
            if (result < multMin) {
                throw new IllegalArgumentException("Protocol error: integer out of range");
            }
            result *= 10;
            if (result < limit + digit) {
                throw new IllegalArgumentException("Protocol error: integer out of range");
            }
            result -= digit;
        }

        return negative ? result : -result;
    }

    private static boolean isSpace(byte b) {
        return b == ' ' || b == '\t';
    }
}

