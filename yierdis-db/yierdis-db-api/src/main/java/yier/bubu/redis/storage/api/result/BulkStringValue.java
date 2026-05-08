package yier.bubu.redis.storage.api.result;

// BulkStringValue：用于表达单个 bulk string（含 null/bytes slice/off-heap slice/long-ascii）并可写入 BulkStringSink。

import yier.bubu.redis.bytes.BytesSlice;

import java.util.Objects;

public final class BulkStringValue {
    private enum Kind {
        NULL,
        BYTES,
        SLICE,
        LONG_ASCII
    }

    private static final BulkStringValue NULL_VALUE = new BulkStringValue(Kind.NULL, null, 0, 0, null, 0L);

    private final Kind kind;
    private final byte[] bytes;
    private final int off;
    private final int len;
    private final BytesSlice slice;
    private final long longValue;

    private BulkStringValue(Kind kind, byte[] bytes, int off, int len, BytesSlice slice, long longValue) {
        this.kind = kind;
        this.bytes = bytes;
        this.off = off;
        this.len = len;
        this.slice = slice;
        this.longValue = longValue;
    }

    public static BulkStringValue nullValue() {
        return NULL_VALUE;
    }

    public static BulkStringValue bytes(byte[] data) {
        if (data == null) {
            return NULL_VALUE;
        }
        return new BulkStringValue(Kind.BYTES, data, 0, data.length, null, 0L);
    }

    public static BulkStringValue bytes(byte[] data, int off, int len) {
        if (data == null) {
            return NULL_VALUE;
        }
        if (off < 0 || len < 0 || off + len > data.length) {
            throw new IndexOutOfBoundsException();
        }
        return new BulkStringValue(Kind.BYTES, data, off, len, null, 0L);
    }

    public static BulkStringValue slice(BytesSlice slice) {
        if (slice == null) {
            return NULL_VALUE;
        }
        return new BulkStringValue(Kind.SLICE, null, 0, 0, slice, 0L);
    }

    public static BulkStringValue longAscii(long value) {
        return new BulkStringValue(Kind.LONG_ASCII, null, 0, 0, null, value);
    }

    public boolean isNull() {
        return kind == Kind.NULL;
    }

    public void writeTo(BulkStringSink out) {
        Objects.requireNonNull(out, "out");
        switch (kind) {
            case NULL -> out.bulkStringNull();
            case BYTES -> out.bulkString(bytes, off, len);
            case SLICE -> out.bulkString(slice);
            case LONG_ASCII -> out.bulkStringLongAscii(longValue);
            default -> throw new IllegalStateException("unknown kind: " + kind);
        }
    }
}

