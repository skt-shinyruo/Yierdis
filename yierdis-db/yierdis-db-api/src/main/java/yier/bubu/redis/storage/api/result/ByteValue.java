package yier.bubu.redis.storage.api.result;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import yier.bubu.redis.bytes.BytesSlice;

public final class ByteValue implements AutoCloseable {
    private enum Kind {
        NULL,
        BYTES,
        SLICE,
        LONG_ASCII,
        OWNED
    }

    private static final ByteValue NULL_VALUE = new ByteValue(
            Kind.NULL, null, 0, 0, null, 0L, 0L, null
    );

    private final Kind kind;
    private final byte[] bytes;
    private final int offset;
    private final int length;
    private final BytesSlice slice;
    private final long longValue;
    private final long retainedMemoryBytes;
    private final AutoCloseable owner;
    private final AtomicBoolean closed = new AtomicBoolean();

    private ByteValue(
            Kind kind,
            byte[] bytes,
            int offset,
            int length,
            BytesSlice slice,
            long longValue,
            long retainedMemoryBytes,
            AutoCloseable owner
    ) {
        this.kind = kind;
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
        this.slice = slice;
        this.longValue = longValue;
        this.retainedMemoryBytes = retainedMemoryBytes;
        this.owner = owner;
    }

    public static ByteValue nullValue() {
        return NULL_VALUE;
    }

    public static ByteValue bytes(byte[] data) {
        return data == null
                ? NULL_VALUE
                : new ByteValue(Kind.BYTES, data, 0, data.length, null, 0L, 0L, null);
    }

    public static ByteValue bytes(byte[] data, int offset, int length) {
        if (data == null) {
            return NULL_VALUE;
        }
        Objects.checkFromIndexSize(offset, length, data.length);
        return new ByteValue(Kind.BYTES, data, offset, length, null, 0L, 0L, null);
    }

    public static ByteValue slice(BytesSlice slice) {
        return slice == null
                ? NULL_VALUE
                : new ByteValue(Kind.SLICE, null, 0, 0, slice, 0L, 0L, null);
    }

    public static ByteValue longAscii(long value) {
        return new ByteValue(Kind.LONG_ASCII, null, 0, 0, null, value, 0L, null);
    }

    public static ByteValue owned(
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
        return new ByteValue(
                Kind.OWNED,
                null,
                0,
                payloadLength,
                slice,
                0L,
                retainedMemoryBytes,
                owner
        );
    }

    public boolean isNull() {
        return kind == Kind.NULL;
    }

    public int payloadLength() {
        return switch (kind) {
            case NULL -> -1;
            case BYTES, OWNED -> length;
            case SLICE -> slice.length();
            case LONG_ASCII -> Long.toString(longValue).length();
        };
    }

    public long retainedMemoryBytes() {
        return retainedMemoryBytes;
    }

    public void emitTo(ByteValueSink out) {
        Objects.requireNonNull(out, "out");
        switch (kind) {
            case NULL -> out.nullValue();
            case BYTES -> out.value(bytes, offset, length);
            case SLICE, OWNED -> out.value(slice);
            case LONG_ASCII -> out.longAscii(longValue);
        }
    }

    @Override
    public void close() {
        if (owner == null || !closed.compareAndSet(false, true)) {
            return;
        }
        try {
            owner.close();
        } catch (RuntimeException | Error failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IllegalStateException("byte value owner close failed", failure);
        }
    }
}
