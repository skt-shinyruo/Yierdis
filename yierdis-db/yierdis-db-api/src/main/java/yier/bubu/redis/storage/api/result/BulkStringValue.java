package yier.bubu.redis.storage.api.result;

// BulkStringValue：用于表达单个 bulk string（含 null/bytes slice/off-heap slice/long-ascii）并可写入 BulkStringSink。

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import yier.bubu.redis.bytes.BytesSlice;

public final class BulkStringValue implements AutoCloseable {
    private enum Kind {
        NULL,
        BYTES,
        SLICE,
        LONG_ASCII,
        OWNED
    }

    private static final BulkStringValue NULL_VALUE = new BulkStringValue(
            Kind.NULL,
            null,
            0,
            0,
            null,
            0L,
            0L,
            null
    );

    private final Kind kind;
    private final byte[] bytes;
    private final int off;
    private final int len;
    private final BytesSlice slice;
    private final long longValue;
    private final long retainedMemoryBytes;
    private final AutoCloseable owner;
    private final AtomicBoolean closed = new AtomicBoolean();

    private BulkStringValue(
            Kind kind,
            byte[] bytes,
            int off,
            int len,
            BytesSlice slice,
            long longValue,
            long retainedMemoryBytes,
            AutoCloseable owner
    ) {
        this.kind = kind;
        this.bytes = bytes;
        this.off = off;
        this.len = len;
        this.slice = slice;
        this.longValue = longValue;
        this.retainedMemoryBytes = retainedMemoryBytes;
        this.owner = owner;
    }

    public static BulkStringValue nullValue() {
        return NULL_VALUE;
    }

    public static BulkStringValue bytes(byte[] data) {
        if (data == null) {
            return NULL_VALUE;
        }
        return new BulkStringValue(Kind.BYTES, data, 0, data.length, null, 0L, 0L, null);
    }

    public static BulkStringValue bytes(byte[] data, int off, int len) {
        if (data == null) {
            return NULL_VALUE;
        }
        if (off < 0 || len < 0 || off > data.length - len) {
            throw new IndexOutOfBoundsException();
        }
        return new BulkStringValue(Kind.BYTES, data, off, len, null, 0L, 0L, null);
    }

    public static BulkStringValue slice(BytesSlice slice) {
        if (slice == null) {
            return NULL_VALUE;
        }
        return new BulkStringValue(Kind.SLICE, null, 0, 0, slice, 0L, 0L, null);
    }

    public static BulkStringValue longAscii(long value) {
        return new BulkStringValue(Kind.LONG_ASCII, null, 0, 0, null, value, 0L, null);
    }

    public static BulkStringValue owned(
            BytesSlice slice,
            int payloadLength,
            long retainedMemoryBytes,
            AutoCloseable owner
    ) {
        Objects.requireNonNull(slice, "slice");
        Objects.requireNonNull(owner, "owner");
        if (payloadLength < 0) {
            throw new IllegalArgumentException("payloadLength must be non-negative");
        }
        if (retainedMemoryBytes < 0L) {
            throw new IllegalArgumentException("retainedMemoryBytes must be non-negative");
        }
        return new BulkStringValue(Kind.OWNED, null, 0, payloadLength, slice, 0L, retainedMemoryBytes, owner);
    }

    public boolean isNull() {
        return kind == Kind.NULL;
    }

    public int payloadLength() {
        return switch (kind) {
            case NULL -> -1;
            case BYTES, OWNED -> len;
            case SLICE -> slice.length();
            case LONG_ASCII -> Long.toString(longValue).length();
        };
    }

    public long retainedMemoryBytes() {
        return retainedMemoryBytes;
    }

    public void writeTo(BulkStringSink out) {
        Objects.requireNonNull(out, "out");
        switch (kind) {
            case NULL -> out.bulkStringNull();
            case BYTES -> out.bulkString(bytes, off, len);
            case SLICE, OWNED -> out.bulkString(slice);
            case LONG_ASCII -> out.bulkStringLongAscii(longValue);
        }
    }

    @Override
    public void close() {
        if (owner == null || !closed.compareAndSet(false, true)) {
            return;
        }
        try {
            owner.close();
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("bulk string owner close failed", e);
        }
    }
}
