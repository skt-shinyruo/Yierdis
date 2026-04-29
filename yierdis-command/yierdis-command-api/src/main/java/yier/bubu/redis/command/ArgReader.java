package yier.bubu.redis.command;

import yier.bubu.redis.contract.ExecutionRequest;

import java.util.Objects;

final class ArgReader {
    private final ExecutionRequest request;

    private ArgReader(ExecutionRequest request) {
        this.request = Objects.requireNonNull(request, "request");
    }

    static ArgReader of(ExecutionRequest request) {
        return new ArgReader(request);
    }

    ExecutionRequest request() {
        return request;
    }

    int argc() {
        return request.argc();
    }

    boolean isNull(int index) {
        return request.isNull(index);
    }

    int len(int index) {
        return request.len(index);
    }

    byte[] bytes(int index) {
        return request.readOnlyByteArray(index);
    }

    boolean is(int index, String literal) {
        return asciiEqualsIgnoreCase(request, index, literal);
    }

    long longAt(int index) {
        return parseLong(request.readOnlyByteArray(index));
    }

    long nonNegativeLongAt(int index) {
        long v = longAt(index);
        if (v < 0) {
            throw new IllegalArgumentException("value is not an integer or out of range");
        }
        return v;
    }

    long positiveLongAt(int index) {
        long v = longAt(index);
        if (v <= 0) {
            throw new IllegalArgumentException("value is not an integer or out of range");
        }
        return v;
    }

    int intClampedAt(int index) {
        long v = longAt(index);
        if (v > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        if (v < Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        }
        return (int) v;
    }

    private static boolean asciiEqualsIgnoreCase(ExecutionRequest request, int argIndex, String literal) {
        if (literal == null) {
            return false;
        }
        if (request.isNull(argIndex)) {
            return false;
        }
        int len = request.len(argIndex);
        if (len != literal.length()) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            int b = request.byteAt(argIndex, i) & 0xFF;
            int c = literal.charAt(i);
            if (b >= 'A' && b <= 'Z') {
                b |= 0x20;
            }
            if (c >= 'A' && c <= 'Z') {
                c |= 0x20;
            }
            if (b != c) {
                return false;
            }
        }
        return true;
    }

    private static long parseLong(byte[] s) {
        if (s == null || s.length == 0) {
            throw new IllegalArgumentException("value is not an integer or out of range");
        }

        int i = 0;
        boolean negative = false;
        byte first = s[0];
        if (first == '-') {
            negative = true;
            i = 1;
        } else if (first == '+') {
            i = 1;
        }
        if (i == s.length) {
            throw new IllegalArgumentException("value is not an integer or out of range");
        }

        long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        long multmin = limit / 10;
        long result = 0;
        while (i < s.length) {
            int digit = s[i++] - '0';
            if (digit < 0 || digit > 9 || result < multmin) {
                throw new IllegalArgumentException("value is not an integer or out of range");
            }
            result *= 10;
            if (result < limit + digit) {
                throw new IllegalArgumentException("value is not an integer or out of range");
            }
            result -= digit;
        }
        return negative ? result : -result;
    }
}
