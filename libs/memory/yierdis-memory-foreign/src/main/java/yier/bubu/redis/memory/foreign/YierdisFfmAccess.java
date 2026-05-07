package yier.bubu.redis.memory.foreign;

import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;

public final class YierdisFfmAccess {
    private YierdisFfmAccess() {
    }

    public static byte getByte(YierdisFfmSpan span, int offset) {
        checkRange(span, offset, 1);
        return span.segment().get(ValueLayout.JAVA_BYTE, offset);
    }

    public static void setByte(YierdisFfmSpan span, int offset, byte value) {
        checkRange(span, offset, 1);
        span.segment().set(ValueLayout.JAVA_BYTE, offset, value);
    }

    public static long getLong(YierdisFfmSpan span, int offset) {
        checkRange(span, offset, Long.BYTES);
        return span.segment().get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
    }

    public static int getInt(YierdisFfmSpan span, int offset) {
        checkRange(span, offset, Integer.BYTES);
        return span.segment().get(ValueLayout.JAVA_INT_UNALIGNED, offset);
    }

    public static void setLong(YierdisFfmSpan span, int offset, long value) {
        checkRange(span, offset, Long.BYTES);
        span.segment().set(ValueLayout.JAVA_LONG_UNALIGNED, offset, value);
    }

    public static void setInt(YierdisFfmSpan span, int offset, int value) {
        checkRange(span, offset, Integer.BYTES);
        span.segment().set(ValueLayout.JAVA_INT_UNALIGNED, offset, value);
    }

    public static void getBytes(YierdisFfmSpan span, int offset, byte[] dst, int dstOff, int len) {
        if (dst == null) {
            throw new IllegalArgumentException("dst must not be null");
        }
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (dstOff < 0 || dstOff + len > dst.length) {
            throw new IndexOutOfBoundsException();
        }
        checkRange(span, offset, len);
        for (int i = 0; i < len; i++) {
            dst[dstOff + i] = getByte(span, offset + i);
        }
    }

    public static void setBytes(YierdisFfmSpan span, int offset, byte[] src, int srcOff, int len) {
        if (src == null) {
            throw new IllegalArgumentException("src must not be null");
        }
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }
        if (srcOff < 0 || srcOff + len > src.length) {
            throw new IndexOutOfBoundsException();
        }
        checkRange(span, offset, len);
        for (int i = 0; i < len; i++) {
            setByte(span, offset + i, src[srcOff + i]);
        }
    }

    public static ByteBuffer asByteBuffer(YierdisFfmSpan span) {
        return span.segment().asByteBuffer();
    }

    public static ByteBuffer asByteBuffer(YierdisFfmSpan span, int offset, int length) {
        return span.slice(offset, length).segment().asByteBuffer();
    }

    private static void checkRange(YierdisFfmSpan span, int offset, int length) {
        if (offset < 0 || offset + length > span.size()) {
            throw new IndexOutOfBoundsException();
        }
    }
}
