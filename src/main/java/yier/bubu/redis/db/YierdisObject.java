package yier.bubu.redis.db;

import java.nio.charset.StandardCharsets;

/**
 * A Redis-like value container: {@link ValueType} + {@link ValueEncoding} + payload.
 * <p>
 * This class is designed to be stored directly in the keyspace (i.e. avoid an extra wrapper per key).
 */
final class YierdisObject {
    ValueType type;
    ValueEncoding encoding;
    Object payload;
    volatile long expireAtMillis;

    long intValue;

    private YierdisObject(ValueType type, ValueEncoding encoding, Object payload, long expireAtMillis) {
        this.type = type;
        this.encoding = encoding;
        this.payload = payload;
        this.expireAtMillis = expireAtMillis;
    }

    static YierdisObject newString(byte[] valueBytes, long expireAtMillis) {
        if (valueBytes == null) {
            return new YierdisObject(ValueType.STRING, ValueEncoding.STRING_RAW, new byte[0], expireAtMillis);
        }

        Long parsed = tryParseLongForIntEncoding(valueBytes, valueBytes.length);
        if (parsed != null) {
            return newStringInt(parsed, expireAtMillis);
        }

        return new YierdisObject(ValueType.STRING, ValueEncoding.STRING_RAW, valueBytes, expireAtMillis);
    }

    static YierdisObject newStringInt(long intValue, long expireAtMillis) {
        YierdisObject o = new YierdisObject(ValueType.STRING, ValueEncoding.STRING_INT, null, expireAtMillis);
        o.intValue = intValue;
        return o;
    }

    static YierdisObject newHash(HashValue hv, long expireAtMillis) {
        return new YierdisObject(ValueType.HASH, hv.encoding(), hv, expireAtMillis);
    }

    static YierdisObject newList(ListValue lv, long expireAtMillis) {
        return new YierdisObject(ValueType.LIST, lv.encoding(), lv, expireAtMillis);
    }

    static YierdisObject newSet(SetValue sv, long expireAtMillis) {
        return new YierdisObject(ValueType.SET, sv.encoding(), sv, expireAtMillis);
    }

    static YierdisObject newZSet(ZSetValue zv, long expireAtMillis) {
        return new YierdisObject(ValueType.ZSET, zv.encoding(), zv, expireAtMillis);
    }

    void overwriteWithString(byte[] valueBytes, long expireAtMillis) {
        YierdisObject next = newString(valueBytes, expireAtMillis);
        this.type = next.type;
        this.encoding = next.encoding;
        this.payload = next.payload;
        this.intValue = next.intValue;
        this.expireAtMillis = expireAtMillis;
    }

    int stringByteLength() {
        if (encoding == ValueEncoding.STRING_INT) {
            return longByteLength(intValue);
        }
        return ((byte[]) payload).length;
    }

    byte[] stringBytesView() {
        if (encoding == ValueEncoding.STRING_INT) {
            return Long.toString(intValue).getBytes(StandardCharsets.US_ASCII);
        }
        return (byte[]) payload;
    }

    int stringAppend(byte[] suffixBytes) {
        if (suffixBytes == null || suffixBytes.length == 0) {
            return stringByteLength();
        }

        ensureStringRaw();
        byte[] current = (byte[]) payload;
        byte[] next = new byte[current.length + suffixBytes.length];
        System.arraycopy(current, 0, next, 0, current.length);
        System.arraycopy(suffixBytes, 0, next, current.length, suffixBytes.length);
        payload = next;
        return next.length;
    }

    long stringIncrBy(long delta) {
        long current;
        if (encoding == ValueEncoding.STRING_INT) {
            current = intValue;
        } else {
            byte[] raw = (byte[]) payload;
            current = parseLongAscii(raw, raw.length);
        }

        long next = safeAdd(current, delta);
        encoding = ValueEncoding.STRING_INT;
        intValue = next;
        payload = null;
        return next;
    }

    void refreshCompositeEncodingFromPayload() {
        if (payload instanceof YierdisValue v) {
            encoding = v.encoding();
        }
    }

    private void ensureStringRaw() {
        if (encoding == ValueEncoding.STRING_RAW) {
            return;
        }

        byte[] raw = Long.toString(intValue).getBytes(StandardCharsets.US_ASCII);
        encoding = ValueEncoding.STRING_RAW;
        payload = raw;
        intValue = 0L;
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

    private static Long tryParseLongForIntEncoding(byte[] buf, int len) {
        long parsed;
        try {
            parsed = parseLongAscii(buf, len);
        } catch (YierdisDb.YierdisCommandException e) {
            return null;
        }
        String canonical = Long.toString(parsed);
        if (canonical.length() != len) {
            return null;
        }
        for (int i = 0; i < len; i++) {
            if ((byte) canonical.charAt(i) != buf[i]) {
                return null;
            }
        }
        return parsed;
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
}
