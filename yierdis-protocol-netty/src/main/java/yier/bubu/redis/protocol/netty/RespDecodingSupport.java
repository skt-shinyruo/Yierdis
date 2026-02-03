package yier.bubu.redis.protocol.netty;

import io.netty.buffer.ByteBuf;

/**
 * RESP 解码公共工具（ByteBuf 层）。
 * <p>
 * 目标：
 * - request/response decoder 复用一套解析核心（减少双轨漂移）
 * - 保持零分配与可回溯（支持半包/粘包）
 */
final class RespDecodingSupport {
    static final byte CR = '\r';
    static final byte LF = '\n';

    private RespDecodingSupport() {
    }

    static int requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be > 0");
        }
        return value;
    }

    static int requireNonNegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
        return value;
    }

    static int indexOfCrlf(ByteBuf in, int start, int maxLineBytes) {
        int maxCrlfStart = start + maxLineBytes;
        int scanLimit = Math.min(in.writerIndex() - 1, maxCrlfStart + 1);
        for (int i = start; i < scanLimit; i++) {
            if (in.getByte(i) == CR && in.getByte(i + 1) == LF) {
                return i;
            }
        }
        return -1;
    }

    static boolean isSingleCharLine(ByteBuf in, int start, int end, byte expected) {
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

    static int parseIntAscii(ByteBuf in, int start, int end) {
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

    static long parseLongAscii(ByteBuf in, int start, int end) {
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
