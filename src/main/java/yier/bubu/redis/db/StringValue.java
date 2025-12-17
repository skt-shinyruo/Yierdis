package yier.bubu.redis.db;

import java.nio.charset.StandardCharsets;

final class StringValue implements YierdisValue {
    private enum Encoding {
        INT,
        RAW
    }

    private Encoding encoding;
    private long intValue;

    private byte[] raw;
    private int rawLen;

    StringValue(String value) {
        set(value);
    }

    StringValue(byte[] value) {
        if (value == null) {
            encoding = Encoding.RAW;
            raw = new byte[0];
            rawLen = 0;
            return;
        }

        Long parsed = tryParseLongAscii(value, value.length);
        if (parsed != null) {
            encoding = Encoding.INT;
            intValue = parsed;
            raw = null;
            rawLen = 0;
            return;
        }

        encoding = Encoding.RAW;
        raw = value;
        rawLen = value.length;
    }

    int byteLength() {
        if (encoding == Encoding.INT) {
            return longByteLength(intValue);
        }
        return rawLen;
    }

    byte[] toBytes() {
        if (encoding == Encoding.INT) {
            return Long.toString(intValue).getBytes(StandardCharsets.US_ASCII);
        }
        byte[] out = new byte[rawLen];
        System.arraycopy(raw, 0, out, 0, rawLen);
        return out;
    }

    int append(String suffix) {
        if (suffix == null || suffix.isEmpty()) {
            return byteLength();
        }

        return append(suffix.getBytes(StandardCharsets.UTF_8));
    }

    int append(byte[] suffixBytes) {
        if (suffixBytes == null || suffixBytes.length == 0) {
            return byteLength();
        }

        ensureRaw();
        ensureCapacity(rawLen + suffixBytes.length);
        System.arraycopy(suffixBytes, 0, raw, rawLen, suffixBytes.length);
        rawLen += suffixBytes.length;
        return rawLen;
    }

    long incrBy(long delta) {
        long current;
        if (encoding == Encoding.INT) {
            current = intValue;
        } else {
            current = parseLongAscii(raw, rawLen);
        }

        long next = safeAdd(current, delta);
        encoding = Encoding.INT;
        intValue = next;
        raw = null;
        rawLen = 0;
        return next;
    }

    private void set(String value) {
        if (value == null) {
            encoding = Encoding.RAW;
            raw = new byte[0];
            rawLen = 0;
            return;
        }

        Long parsed = tryParseLong(value);
        if (parsed != null) {
            encoding = Encoding.INT;
            intValue = parsed;
            raw = null;
            rawLen = 0;
            return;
        }

        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        encoding = Encoding.RAW;
        raw = bytes;
        rawLen = bytes.length;
    }

    private void ensureRaw() {
        if (encoding == Encoding.RAW) {
            if (raw == null) {
                raw = new byte[0];
                rawLen = 0;
            }
            return;
        }
        byte[] bytes = Long.toString(intValue).getBytes(StandardCharsets.US_ASCII);
        encoding = Encoding.RAW;
        raw = bytes;
        rawLen = bytes.length;
    }

    private void ensureCapacity(int desiredLen) {
        if (raw.length >= desiredLen) {
            return;
        }

        int newCap = nextCapacity(desiredLen);
        byte[] next = new byte[newCap];
        if (rawLen > 0) {
            System.arraycopy(raw, 0, next, 0, rawLen);
        }
        raw = next;
    }

    private static int nextCapacity(int desiredLen) {
        final int MB = 1024 * 1024;
        if (desiredLen < MB) {
            return desiredLen * 2;
        }
        return desiredLen + MB;
    }

    private static Long tryParseLong(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static int longByteLength(long v) {
        if (v == Long.MIN_VALUE) {
            return 20;
        }
        long x = v < 0 ? -v : v;
        int digits = 1;
        while (x >= 10) {
            x /= 10;
            digits++;
        }
        return v < 0 ? digits + 1 : digits;
    }

    private static long parseLongAscii(byte[] buf, int len) {
        if (len <= 0) {
            throw new YierdisDb.YierdisCommandException("ERR value is not an integer or out of range");
        }

        int i = 0;
        boolean negative = false;
        byte first = buf[0];
        if (first == '-' || first == '+') {
            negative = first == '-';
            i = 1;
            if (i == len) {
                throw new YierdisDb.YierdisCommandException("ERR value is not an integer or out of range");
            }
        }

        long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        long multMin = limit / 10;
        long result = 0;

        while (i < len) {
            int digit = buf[i++] - '0';
            if (digit < 0 || digit > 9) {
                throw new YierdisDb.YierdisCommandException("ERR value is not an integer or out of range");
            }
            if (result < multMin) {
                throw new YierdisDb.YierdisCommandException("ERR value is not an integer or out of range");
            }
            result *= 10;
            if (result < limit + digit) {
                throw new YierdisDb.YierdisCommandException("ERR value is not an integer or out of range");
            }
            result -= digit;
        }

        return negative ? result : -result;
    }

    private static Long tryParseLongAscii(byte[] buf, int len) {
        try {
            return parseLongAscii(buf, len);
        } catch (YierdisDb.YierdisCommandException e) {
            return null;
        }
    }

    private static long safeAdd(long a, long b) {
        if (b > 0 && a > Long.MAX_VALUE - b) {
            throw new YierdisDb.YierdisCommandException("ERR value is not an integer or out of range");
        }
        if (b < 0 && a < Long.MIN_VALUE - b) {
            throw new YierdisDb.YierdisCommandException("ERR value is not an integer or out of range");
        }
        return a + b;
    }

    @Override
    public ValueType type() {
        return ValueType.STRING;
    }
}
