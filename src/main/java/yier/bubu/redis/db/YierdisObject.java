package yier.bubu.redis.db;

import java.util.Arrays;
import java.nio.charset.StandardCharsets;

/**
 * A Redis-like value container: {@link ValueType} + {@link ValueEncoding} + payload.
 * <p>
 * This class is designed to be stored directly in the keyspace (i.e. avoid an extra wrapper per key).
 */
final class YierdisObject {
    // Redis uses EMBSTR for small strings (<= 44 bytes) and RAW for larger strings.
    // In Java we approximate this by tracking the encoding and forcing a conversion to RAW
    // before any in-place growth.
    private static final int EMBSTR_MAX_BYTES = 44;

    ValueType type;
    ValueEncoding encoding;
    Object payload;

    int rawLen;
    long intValue;

    long intBytesCacheFor;
    byte[] intBytesCache;

    private YierdisObject(ValueType type, ValueEncoding encoding, Object payload) {
        this.type = type;
        this.encoding = encoding;
        this.payload = payload;
    }

    static YierdisObject newString(byte[] valueBytes) {
        if (valueBytes == null) {
            YierdisObject o = new YierdisObject(ValueType.STRING, ValueEncoding.STRING_EMBSTR, new byte[0]);
            o.rawLen = 0;
            return o;
        }

        Long parsed = tryParseLongForIntEncoding(valueBytes, valueBytes.length);
        if (parsed != null) {
            return newStringInt(parsed);
        }

        ValueEncoding enc = valueBytes.length <= EMBSTR_MAX_BYTES ? ValueEncoding.STRING_EMBSTR : ValueEncoding.STRING_RAW;
        YierdisObject o = new YierdisObject(ValueType.STRING, enc, valueBytes);
        o.rawLen = valueBytes.length;
        return o;
    }

    static YierdisObject newStringInt(long intValue) {
        YierdisObject o = new YierdisObject(ValueType.STRING, ValueEncoding.STRING_INT, null);
        o.intValue = intValue;
        o.rawLen = 0;
        return o;
    }

    static YierdisObject newHash(HashValue hv) {
        return new YierdisObject(ValueType.HASH, hv.encoding(), hv);
    }

    static YierdisObject newList(ListValue lv) {
        return new YierdisObject(ValueType.LIST, lv.encoding(), lv);
    }

    static YierdisObject newSet(SetValue sv) {
        return new YierdisObject(ValueType.SET, sv.encoding(), sv);
    }

    static YierdisObject newZSet(ZSetValue zv) {
        return new YierdisObject(ValueType.ZSET, zv.encoding(), zv);
    }

    void overwriteWithString(byte[] valueBytes) {
        YierdisObject next = newString(valueBytes);
        this.type = next.type;
        this.encoding = next.encoding;
        this.payload = next.payload;
        this.intValue = next.intValue;
        this.rawLen = next.rawLen;
        this.intBytesCache = null;
        this.intBytesCacheFor = 0L;
    }

    int stringByteLength() {
        if (encoding == ValueEncoding.STRING_INT) {
            return longByteLength(intValue);
        }
        return rawLen;
    }

    byte[] stringBytesView() {
        if (encoding == ValueEncoding.STRING_INT) {
            byte[] cached = intBytesCache;
            if (cached == null || intBytesCacheFor != intValue) {
                cached = Long.toString(intValue).getBytes(StandardCharsets.US_ASCII);
                intBytesCache = cached;
                intBytesCacheFor = intValue;
            }
            return cached;
        }
        byte[] buf = (byte[]) payload;
        if (rawLen == buf.length) {
            return buf;
        }
        return Arrays.copyOf(buf, rawLen);
    }

    int stringAppend(byte[] suffixBytes) {
        if (suffixBytes == null || suffixBytes.length == 0) {
            return stringByteLength();
        }

        ensureStringRawForAppend(suffixBytes.length);
        byte[] buf = (byte[]) payload;
        System.arraycopy(suffixBytes, 0, buf, rawLen, suffixBytes.length);
        rawLen += suffixBytes.length;
        return rawLen;
    }

    long stringIncrBy(long delta) {
        long current;
        if (encoding == ValueEncoding.STRING_INT) {
            current = intValue;
        } else {
            byte[] raw = (byte[]) payload;
            current = parseLongAscii(raw, rawLen);
        }

        long next = safeAdd(current, delta);
        encoding = ValueEncoding.STRING_INT;
        intValue = next;
        intBytesCache = null;
        intBytesCacheFor = 0L;
        payload = null;
        rawLen = 0;
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
        if (encoding == ValueEncoding.STRING_EMBSTR) {
            // EMBSTR is already a compact byte[]; treat it as RAW for subsequent mutations.
            encoding = ValueEncoding.STRING_RAW;
            return;
        }
        if (encoding != ValueEncoding.STRING_INT) {
            throw new IllegalStateException("unexpected string encoding: " + encoding);
        }

        byte[] raw = Long.toString(intValue).getBytes(StandardCharsets.US_ASCII);
        encoding = ValueEncoding.STRING_RAW;
        payload = raw;
        rawLen = raw.length;
        intValue = 0L;
        intBytesCache = null;
        intBytesCacheFor = 0L;
    }

    private void ensureStringRawForAppend(int additionalBytes) {
        if (additionalBytes < 0) {
            throw new IllegalArgumentException("additionalBytes must be >= 0");
        }
        if (encoding == ValueEncoding.STRING_INT) {
            byte[] raw = Long.toString(intValue).getBytes(StandardCharsets.US_ASCII);
            int required = raw.length + additionalBytes;
            int cap = required == raw.length ? raw.length : nextCapacity(raw.length, required);
            byte[] buf = cap == raw.length ? raw : Arrays.copyOf(raw, cap);
            encoding = ValueEncoding.STRING_RAW;
            payload = buf;
            rawLen = raw.length;
            intValue = 0L;
            intBytesCache = null;
            intBytesCacheFor = 0L;
            return;
        }

        ensureStringRaw();
        byte[] buf = (byte[]) payload;
        int required = rawLen + additionalBytes;
        if (buf.length >= required) {
            return;
        }
        int cap = nextCapacity(buf.length, required);
        payload = Arrays.copyOf(buf, cap);
    }

    private static int nextCapacity(int current, int required) {
        int cap = Math.max(16, current);
        while (cap < required) {
            int next = cap < 1024 * 1024 ? (cap << 1) : (cap + 1024 * 1024);
            if (next <= cap) {
                return required;
            }
            cap = next;
        }
        return cap;
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
