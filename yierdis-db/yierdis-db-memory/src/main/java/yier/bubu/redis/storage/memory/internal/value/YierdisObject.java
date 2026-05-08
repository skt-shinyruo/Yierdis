package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.memory.api.OffHeapAllocator;
import yier.bubu.redis.memory.api.OffHeapBuf;
import yier.bubu.redis.memory.api.OffHeapSlice;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.storage.api.YierdisCommandException;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A Redis-like value container: {@link ValueType} + {@link ValueEncoding} + payload.
 * <p>
 * This class is designed to be stored directly in the keyspace (i.e. avoid an extra wrapper per key).
 */
public final class YierdisObject {
    // Redis uses EMBSTR for small strings (<= 44 bytes) and RAW for larger strings.
    // In Java we approximate this by tracking the encoding and forcing a conversion to RAW
    // before any in-place growth.
    private static final int EMBSTR_MAX_BYTES = 44;
    // 与 Redis 对齐：string 最大 512MB（避免超大 offset 触发不可控分配/清零成本）。
    private static final int MAX_STRING_BYTES = 512 * 1024 * 1024;
    private static final ThreadLocal<byte[]> TL_COPY_BUF = ThreadLocal.withInitial(() -> new byte[8 * 1024]);
    private static final ThreadLocal<byte[]> TL_ZERO_BUF = ThreadLocal.withInitial(() -> new byte[8 * 1024]);

    public ValueType type;
    public ValueEncoding encoding;
    public Object payload;

    public int rawLen;
    public long intValue;

    public long intBytesCacheFor;
    public byte[] intBytesCache;

    public long estimatedBytes;
    public long lruClock;

    private YierdisObject(ValueType type, ValueEncoding encoding, Object payload) {
        this.type = type;
        this.encoding = encoding;
        this.payload = payload;
    }

    public static YierdisObject newString(byte[] valueBytes) {
        return newString(null, valueBytes);
    }

    public static YierdisObject newString(OffHeapAllocator offHeapAllocator, byte[] valueBytes) {
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
        Object payload = valueBytes;
        if (offHeapAllocator != null && valueBytes.length > 0) {
            OffHeapBuf buf = offHeapAllocator.allocate(valueBytes.length);
            buf.setBytes(0, valueBytes, 0, valueBytes.length);
            payload = buf;
        }
        YierdisObject o = new YierdisObject(ValueType.STRING, enc, payload);
        o.rawLen = valueBytes.length;
        return o;
    }

    public static YierdisObject newString(OffHeapAllocator offHeapAllocator, BytesSlice value) {
        if (value == null) {
            return newString(offHeapAllocator, (byte[]) null);
        }

        int len = value.length();
        if (len <= 0) {
            return newString(offHeapAllocator, (byte[]) null);
        }

        Long parsed = tryParseLongForIntEncoding(value);
        if (parsed != null) {
            return newStringInt(parsed);
        }

        ValueEncoding enc = len <= EMBSTR_MAX_BYTES ? ValueEncoding.STRING_EMBSTR : ValueEncoding.STRING_RAW;
        Object payload;
        if (offHeapAllocator != null) {
            OffHeapBuf buf = offHeapAllocator.allocate(len);
            boolean ok = false;
            try {
                buf.setBytes(0, value, 0, len);
                payload = buf;
                ok = true;
            } finally {
                if (!ok) {
                    buf.close();
                }
            }
        } else {
            byte[] raw = new byte[len];
            value.getBytes(0, raw, 0, len);
            payload = raw;
        }

        YierdisObject o = new YierdisObject(ValueType.STRING, enc, payload);
        o.rawLen = len;
        return o;
    }

    public static YierdisObject newStringInt(long intValue) {
        YierdisObject o = new YierdisObject(ValueType.STRING, ValueEncoding.STRING_INT, null);
        o.intValue = intValue;
        o.rawLen = 0;
        return o;
    }

    public static YierdisObject newHash(HashValue hv) {
        return new YierdisObject(ValueType.HASH, hv.encoding(), hv);
    }

    public static YierdisObject newList(ListValue lv) {
        return new YierdisObject(ValueType.LIST, lv.encoding(), lv);
    }

    public static YierdisObject newSet(SetValue sv) {
        return new YierdisObject(ValueType.SET, sv.encoding(), sv);
    }

    public static YierdisObject newZSet(ZSetValue zv) {
        return new YierdisObject(ValueType.ZSET, zv.encoding(), zv);
    }

    public void overwriteWithString(byte[] valueBytes) {
        overwriteWithString(null, valueBytes);
    }

    public void overwriteWithString(OffHeapAllocator offHeapAllocator, byte[] valueBytes) {
        // Fast-path: reuse an existing off-heap buffer for SET-overwrite to avoid needing
        // "old + new" bytes at the same time under a hard cap.
        if (payload instanceof OffHeapBuf current && offHeapAllocator != null) {
            if (valueBytes == null) {
                current.close();
                this.type = ValueType.STRING;
                this.encoding = ValueEncoding.STRING_EMBSTR;
                this.payload = new byte[0];
                this.rawLen = 0;
                this.intValue = 0L;
                this.intBytesCache = null;
                this.intBytesCacheFor = 0L;
                return;
            }

            Long parsed = tryParseLongForIntEncoding(valueBytes, valueBytes.length);
            if (parsed != null) {
                current.close();
                this.type = ValueType.STRING;
                this.encoding = ValueEncoding.STRING_INT;
                this.payload = null;
                this.rawLen = 0;
                this.intValue = parsed;
                this.intBytesCache = null;
                this.intBytesCacheFor = 0L;
                return;
            }

            int nextLen = valueBytes.length;
            ValueEncoding nextEnc = nextLen <= EMBSTR_MAX_BYTES ? ValueEncoding.STRING_EMBSTR : ValueEncoding.STRING_RAW;
            if (nextLen > 0 && current.capacity() >= nextLen) {
                current.setBytes(0, valueBytes, 0, nextLen);
                this.type = ValueType.STRING;
                this.encoding = nextEnc;
                this.rawLen = nextLen;
                this.intValue = 0L;
                this.intBytesCache = null;
                this.intBytesCacheFor = 0L;
                return;
            }

            Object nextPayload;
            if (nextLen == 0) {
                nextPayload = new byte[0];
            } else {
                OffHeapBuf nextBuf = offHeapAllocator.allocate(nextLen);
                nextBuf.setBytes(0, valueBytes, 0, nextLen);
                nextPayload = nextBuf;
            }
            current.close();
            this.type = ValueType.STRING;
            this.encoding = nextEnc;
            this.payload = nextPayload;
            this.rawLen = nextLen;
            this.intValue = 0L;
            this.intBytesCache = null;
            this.intBytesCacheFor = 0L;
            return;
        }

        YierdisObject next = newString(offHeapAllocator, valueBytes);
        releasePayloadIfAny();
        this.type = next.type;
        this.encoding = next.encoding;
        this.payload = next.payload;
        this.intValue = next.intValue;
        this.rawLen = next.rawLen;
        this.intBytesCache = null;
        this.intBytesCacheFor = 0L;
    }

    public void overwriteWithString(OffHeapAllocator offHeapAllocator, BytesSlice value) {
        int len = value == null ? 0 : value.length();
        // Fast-path: reuse an existing off-heap buffer for SET-overwrite to avoid needing
        // "old + new" bytes at the same time under a hard cap.
        if (payload instanceof OffHeapBuf current && offHeapAllocator != null) {
            if (len <= 0) {
                current.close();
                this.type = ValueType.STRING;
                this.encoding = ValueEncoding.STRING_EMBSTR;
                this.payload = new byte[0];
                this.rawLen = 0;
                this.intValue = 0L;
                this.intBytesCache = null;
                this.intBytesCacheFor = 0L;
                return;
            }

            Long parsed = tryParseLongForIntEncoding(value);
            if (parsed != null) {
                current.close();
                this.type = ValueType.STRING;
                this.encoding = ValueEncoding.STRING_INT;
                this.payload = null;
                this.rawLen = 0;
                this.intValue = parsed;
                this.intBytesCache = null;
                this.intBytesCacheFor = 0L;
                return;
            }

            ValueEncoding nextEnc = len <= EMBSTR_MAX_BYTES ? ValueEncoding.STRING_EMBSTR : ValueEncoding.STRING_RAW;
            if (len > 0 && current.capacity() >= len) {
                current.setBytes(0, value, 0, len);
                this.type = ValueType.STRING;
                this.encoding = nextEnc;
                this.payload = current;
                this.rawLen = len;
                this.intValue = 0L;
                this.intBytesCache = null;
                this.intBytesCacheFor = 0L;
                return;
            }

            Object nextPayload;
            if (len == 0) {
                nextPayload = new byte[0];
            } else {
                OffHeapBuf next = offHeapAllocator.allocate(len);
                boolean ok = false;
                try {
                    next.setBytes(0, value, 0, len);
                    nextPayload = next;
                    ok = true;
                } finally {
                    if (!ok) {
                        next.close();
                    }
                }
            }
            current.close();
            this.type = ValueType.STRING;
            this.encoding = nextEnc;
            this.payload = nextPayload;
            this.rawLen = len;
            this.intValue = 0L;
            this.intBytesCache = null;
            this.intBytesCacheFor = 0L;
            return;
        }

        // Fallback: allocate a new payload and replace.
        if (len <= 0) {
            releaseStringPayloadIfAny();
            this.type = ValueType.STRING;
            this.encoding = ValueEncoding.STRING_EMBSTR;
            this.payload = new byte[0];
            this.rawLen = 0;
            this.intValue = 0L;
            this.intBytesCache = null;
            this.intBytesCacheFor = 0L;
            return;
        }

        Long parsed = tryParseLongForIntEncoding(value);
        if (parsed != null) {
            releaseStringPayloadIfAny();
            this.type = ValueType.STRING;
            this.encoding = ValueEncoding.STRING_INT;
            this.payload = null;
            this.rawLen = 0;
            this.intValue = parsed;
            this.intBytesCache = null;
            this.intBytesCacheFor = 0L;
            return;
        }

        ValueEncoding nextEnc = len <= EMBSTR_MAX_BYTES ? ValueEncoding.STRING_EMBSTR : ValueEncoding.STRING_RAW;
        Object nextPayload;
        if (offHeapAllocator != null) {
            OffHeapBuf next = offHeapAllocator.allocate(len);
            boolean ok = false;
            try {
                next.setBytes(0, value, 0, len);
                nextPayload = next;
                ok = true;
            } finally {
                if (!ok) {
                    next.close();
                }
            }
        } else {
            byte[] raw = new byte[len];
            value.getBytes(0, raw, 0, len);
            nextPayload = raw;
        }

        releaseStringPayloadIfAny();
        this.type = ValueType.STRING;
        this.encoding = nextEnc;
        this.payload = nextPayload;
        this.rawLen = len;
        this.intValue = 0L;
        this.intBytesCache = null;
        this.intBytesCacheFor = 0L;
    }

    public int stringByteLength() {
        if (encoding == ValueEncoding.STRING_INT) {
            return longByteLength(intValue);
        }
        return rawLen;
    }

    public byte[] stringBytesView() {
        if (encoding == ValueEncoding.STRING_INT) {
            byte[] cached = intBytesCache;
            if (cached == null || intBytesCacheFor != intValue) {
                cached = Long.toString(intValue).getBytes(StandardCharsets.US_ASCII);
                intBytesCache = cached;
                intBytesCacheFor = intValue;
            }
            return cached;
        }
        if (payload instanceof byte[] buf) {
            if (rawLen == buf.length) {
                return buf;
            }
            return Arrays.copyOf(buf, rawLen);
        }
        if (payload instanceof OffHeapBuf offHeapBuf) {
            if (rawLen == 0) {
                return new byte[0];
            }
            byte[] out = new byte[rawLen];
            offHeapBuf.getBytes(0, out, 0, rawLen);
            return out;
        }
        return new byte[0];
    }

    public OffHeapSlice stringOffHeapSlice() {
        if (payload instanceof OffHeapBuf buf) {
            return buf.slice(0, rawLen);
        }
        return null;
    }

    public int stringGetBit(long offset) {
        if (offset < 0) {
            throw new YierdisCommandException("ERR bit offset is not an integer or out of range");
        }
        long byteIndexLong = offset >>> 3;
        if (byteIndexLong > Integer.MAX_VALUE) {
            throw new YierdisCommandException("ERR bit offset is not an integer or out of range");
        }
        int byteIndex = (int) byteIndexLong;
        int bit = (int) (offset & 7);
        int mask = 1 << (7 - bit);

        if (encoding == ValueEncoding.STRING_INT) {
            byte[] view = stringBytesView();
            if (byteIndex >= view.length) {
                return 0;
            }
            return (view[byteIndex] & mask) == 0 ? 0 : 1;
        }

        if (byteIndex >= rawLen) {
            return 0;
        }
        byte b = stringByteAt(byteIndex);
        return (b & mask) == 0 ? 0 : 1;
    }

    public int stringSetBit(OffHeapAllocator offHeapAllocator, long offset, int value) {
        if (value != 0 && value != 1) {
            throw new YierdisCommandException("ERR bit is not an integer or out of range");
        }
        if (offset < 0) {
            throw new YierdisCommandException("ERR bit offset is not an integer or out of range");
        }
        long byteIndexLong = offset >>> 3;
        if (byteIndexLong > Integer.MAX_VALUE) {
            throw new YierdisCommandException("ERR bit offset is not an integer or out of range");
        }
        int byteIndex = (int) byteIndexLong;
        int requiredLen = byteIndex + 1;
        if (requiredLen > MAX_STRING_BYTES) {
            throw new YierdisCommandException("ERR string exceeds maximum allowed size");
        }

        stringEnsureLength(offHeapAllocator, requiredLen);

        int bit = (int) (offset & 7);
        int mask = 1 << (7 - bit);
        byte oldByte = stringByteAt(byteIndex);
        int oldBit = (oldByte & mask) == 0 ? 0 : 1;
        byte nextByte;
        if (value == 1) {
            nextByte = (byte) (oldByte | mask);
        } else {
            nextByte = (byte) (oldByte & ~mask);
        }
        if (nextByte != oldByte) {
            stringSetByteAt(byteIndex, nextByte);
        }
        return oldBit;
    }

    public void stringEnsureLength(OffHeapAllocator offHeapAllocator, int requiredLen) {
        if (requiredLen < 0) {
            throw new IllegalArgumentException("requiredLen must be >= 0");
        }
        if (requiredLen > MAX_STRING_BYTES) {
            throw new YierdisCommandException("ERR string exceeds maximum allowed size");
        }

        ensureStringRawForSize(offHeapAllocator, requiredLen);
        if (requiredLen <= rawLen) {
            return;
        }

        int oldLen = rawLen;
        zeroFillStringRange(oldLen, requiredLen);
        rawLen = requiredLen;
    }

    public byte stringByteAt(int index) {
        if (index < 0 || index >= rawLen) {
            throw new IndexOutOfBoundsException();
        }
        if (payload instanceof byte[] buf) {
            return buf[index];
        }
        if (payload instanceof OffHeapBuf buf) {
            return buf.getByte(index);
        }
        throw new IllegalStateException("unexpected string payload: " + payload);
    }

    public void stringSetByteAt(int index, byte value) {
        if (index < 0 || index >= rawLen) {
            throw new IndexOutOfBoundsException();
        }
        if (payload instanceof byte[] buf) {
            buf[index] = value;
            return;
        }
        if (payload instanceof OffHeapBuf buf) {
            buf.setByte(index, value);
            return;
        }
        throw new IllegalStateException("unexpected string payload: " + payload);
    }

    private void zeroFillStringRange(int from, int toExclusive) {
        if (from >= toExclusive) {
            return;
        }
        if (from < 0) {
            throw new IllegalArgumentException("from must be >= 0");
        }
        if (toExclusive < 0) {
            throw new IllegalArgumentException("toExclusive must be >= 0");
        }
        if (payload instanceof byte[] buf) {
            Arrays.fill(buf, from, toExclusive, (byte) 0);
            return;
        }
        if (payload instanceof OffHeapBuf buf) {
            byte[] zeros = TL_ZERO_BUF.get();
            int remaining = toExclusive - from;
            int off = from;
            while (remaining > 0) {
                int chunk = Math.min(remaining, zeros.length);
                buf.setBytes(off, zeros, 0, chunk);
                off += chunk;
                remaining -= chunk;
            }
            return;
        }
        throw new IllegalStateException("unexpected string payload: " + payload);
    }

    public int stringAppend(OffHeapAllocator offHeapAllocator, byte[] suffixBytes) {
        if (suffixBytes == null || suffixBytes.length == 0) {
            return stringByteLength();
        }

        ensureStringRawForAppend(offHeapAllocator, suffixBytes.length);
        if (payload instanceof byte[] buf) {
            System.arraycopy(suffixBytes, 0, buf, rawLen, suffixBytes.length);
            rawLen += suffixBytes.length;
            return rawLen;
        }
        if (payload instanceof OffHeapBuf buf) {
            buf.setBytes(rawLen, suffixBytes, 0, suffixBytes.length);
            rawLen += suffixBytes.length;
            return rawLen;
        }
        throw new IllegalStateException("unexpected string payload: " + payload);
    }

    public int stringAppend(OffHeapAllocator offHeapAllocator, BytesSlice suffix) {
        if (suffix == null) {
            return stringByteLength();
        }
        int len = suffix.length();
        if (len <= 0) {
            return stringByteLength();
        }

        ensureStringRawForAppend(offHeapAllocator, len);
        if (payload instanceof byte[] buf) {
            suffix.getBytes(0, buf, rawLen, len);
            rawLen += len;
            return rawLen;
        }
        if (payload instanceof OffHeapBuf buf) {
            buf.setBytes(rawLen, suffix, 0, len);
            rawLen += len;
            return rawLen;
        }
        throw new IllegalStateException("unexpected string payload: " + payload);
    }

    public long stringIncrBy(OffHeapAllocator offHeapAllocator, long delta) {
        long current;
        if (encoding == ValueEncoding.STRING_INT) {
            current = intValue;
        } else {
            current = parseLongAsciiPayload(rawLen);
        }

        long next = safeAdd(current, delta);
        encoding = ValueEncoding.STRING_INT;
        intValue = next;
        intBytesCache = null;
        intBytesCacheFor = 0L;
        releaseStringPayloadIfAny();
        payload = null;
        rawLen = 0;
        return next;
    }

    public void refreshCompositeEncodingFromPayload() {
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

    private void ensureStringRawForAppend(OffHeapAllocator offHeapAllocator, int additionalBytes) {
        if (additionalBytes < 0) {
            throw new IllegalArgumentException("additionalBytes must be >= 0");
        }
        if (encoding == ValueEncoding.STRING_INT) {
            byte[] raw = Long.toString(intValue).getBytes(StandardCharsets.US_ASCII);
            int required = raw.length + additionalBytes;
            int cap = required == raw.length ? raw.length : nextCapacity(raw.length, required);
            Object nextPayload = cap == raw.length ? raw : Arrays.copyOf(raw, cap);
            if (offHeapAllocator != null && cap > 0) {
                OffHeapBuf nextBuf = offHeapAllocator.allocate(cap);
                nextBuf.setBytes(0, raw, 0, raw.length);
                nextPayload = nextBuf;
            }
            encoding = ValueEncoding.STRING_RAW;
            payload = nextPayload;
            rawLen = raw.length;
            intValue = 0L;
            intBytesCache = null;
            intBytesCacheFor = 0L;
            return;
        }

        ensureStringRaw();
        int required = rawLen + additionalBytes;
        if (payload instanceof byte[] buf) {
            if (buf.length >= required) {
                return;
            }
            int cap = nextCapacity(buf.length, required);
            payload = Arrays.copyOf(buf, cap);
            return;
        }
        if (payload instanceof OffHeapBuf buf) {
            if (buf.capacity() >= required) {
                return;
            }
            if (offHeapAllocator == null) {
                throw new IllegalStateException("offHeapAllocator is required for off-heap string growth");
            }
            int cap = nextCapacity(buf.capacity(), required);
            payload = resizeOffHeapString(buf, offHeapAllocator, cap, rawLen);
            return;
        }
        throw new IllegalStateException("unexpected string payload: " + payload);
    }

    private void ensureStringRawForSize(OffHeapAllocator offHeapAllocator, int requiredLen) {
        if (requiredLen < 0) {
            throw new IllegalArgumentException("requiredLen must be >= 0");
        }
        if (requiredLen > MAX_STRING_BYTES) {
            throw new YierdisCommandException("ERR string exceeds maximum allowed size");
        }
        int currentLen = stringByteLength();
        int additional = Math.max(0, requiredLen - currentLen);
        ensureStringRawForAppend(offHeapAllocator, additional);
    }

    private static OffHeapBuf resizeOffHeapString(OffHeapBuf current,
                                                  OffHeapAllocator allocator,
                                                  int newCapacity,
                                                  int usedBytes) {
        if (newCapacity < 0) {
            throw new IllegalArgumentException("newCapacity must be >= 0");
        }
        if (usedBytes < 0) {
            throw new IllegalArgumentException("usedBytes must be >= 0");
        }
        if (newCapacity < usedBytes) {
            throw new IllegalArgumentException("newCapacity must be >= usedBytes");
        }

        OffHeapBuf next = allocator.allocate(Math.max(1, newCapacity));
        try {
            if (usedBytes > 0) {
                copyOffHeapToOffHeap(current, next, usedBytes);
            }
        } catch (RuntimeException e) {
            next.close();
            throw e;
        }
        current.close();
        return next;
    }

    private static void copyOffHeapToOffHeap(OffHeapBuf src, OffHeapBuf dst, int len) {
        byte[] scratch = TL_COPY_BUF.get();
        int remaining = len;
        int off = 0;
        while (remaining > 0) {
            int chunk = Math.min(remaining, scratch.length);
            src.getBytes(off, scratch, 0, chunk);
            dst.setBytes(off, scratch, 0, chunk);
            off += chunk;
            remaining -= chunk;
        }
    }

    private long parseLongAsciiPayload(int len) {
        if (payload instanceof byte[] raw) {
            return parseLongAscii(raw, len);
        }
        if (payload instanceof OffHeapBuf buf) {
            return parseLongAscii(buf, len);
        }
        throw new YierdisCommandException("ERR value is not an integer or out of range");
    }

    private static long parseLongAscii(OffHeapBuf buf, int len) {
        if (len <= 0) {
            throw new YierdisCommandException("ERR value is not an integer or out of range");
        }

        int i = 0;
        boolean negative = false;
        byte first = buf.getByte(0);
        if (first == '-') {
            negative = true;
            i = 1;
        } else if (first == '+') {
            i = 1;
        }
        if (i >= len) {
            throw new YierdisCommandException("ERR value is not an integer or out of range");
        }

        long value = 0L;
        for (; i < len; i++) {
            byte b = buf.getByte(i);
            if (b < '0' || b > '9') {
                throw new YierdisCommandException("ERR value is not an integer or out of range");
            }
            int digit = b - '0';
            if (value > (Long.MAX_VALUE - digit) / 10L) {
                throw new YierdisCommandException("ERR value is not an integer or out of range");
            }
            value = value * 10L + digit;
        }
        return negative ? -value : value;
    }

    public void releaseStringPayloadIfAny() {
        if (payload instanceof OffHeapBuf buf) {
            buf.close();
        }
    }

    public void releasePayloadIfAny() {
        releaseStringPayloadIfAny();
        if (payload instanceof YierdisValue v) {
            v.close();
        }
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
            throw new YierdisCommandException("ERR value is not an integer or out of range");
        }
        if (b < 0 && a < Long.MIN_VALUE - b) {
            throw new YierdisCommandException("ERR value is not an integer or out of range");
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
        } catch (YierdisCommandException e) {
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

    private static Long tryParseLongForIntEncoding(BytesSlice slice) {
        if (slice == null) {
            return null;
        }
        int len = slice.length();
        if (len <= 0) {
            return null;
        }
        long parsed;
        try {
            parsed = parseLongAscii(slice, len);
        } catch (YierdisCommandException e) {
            return null;
        }
        String canonical = Long.toString(parsed);
        if (canonical.length() != len) {
            return null;
        }
        for (int i = 0; i < len; i++) {
            if ((byte) canonical.charAt(i) != slice.getByte(i)) {
                return null;
            }
        }
        return parsed;
    }

    private static long parseLongAscii(BytesSlice slice, int len) {
        if (len <= 0) {
            throw new YierdisCommandException("ERR value is not an integer or out of range");
        }

        int i = 0;
        boolean negative = false;
        byte first = slice.getByte(0);
        if (first == '-' || first == '+') {
            negative = first == '-';
            i = 1;
            if (i == len) {
                throw new YierdisCommandException("ERR value is not an integer or out of range");
            }
        }

        long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        long multMin = limit / 10;
        long result = 0;

        while (i < len) {
            int digit = slice.getByte(i++) - '0';
            if (digit < 0 || digit > 9) {
                throw new YierdisCommandException("ERR value is not an integer or out of range");
            }
            if (result < multMin) {
                throw new YierdisCommandException("ERR value is not an integer or out of range");
            }
            result *= 10;
            if (result < limit + digit) {
                throw new YierdisCommandException("ERR value is not an integer or out of range");
            }
            result -= digit;
        }

        return negative ? result : -result;
    }

    private static long parseLongAscii(byte[] buf, int len) {
        if (len <= 0) {
            throw new YierdisCommandException("ERR value is not an integer or out of range");
        }

        int i = 0;
        boolean negative = false;
        byte first = buf[0];
        if (first == '-' || first == '+') {
            negative = first == '-';
            i = 1;
            if (i == len) {
                throw new YierdisCommandException("ERR value is not an integer or out of range");
            }
        }

        long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        long multMin = limit / 10;
        long result = 0;

        while (i < len) {
            int digit = buf[i++] - '0';
            if (digit < 0 || digit > 9) {
                throw new YierdisCommandException("ERR value is not an integer or out of range");
            }
            if (result < multMin) {
                throw new YierdisCommandException("ERR value is not an integer or out of range");
            }
            result *= 10;
            if (result < limit + digit) {
                throw new YierdisCommandException("ERR value is not an integer or out of range");
            }
            result -= digit;
        }

        return negative ? result : -result;
    }
}
