package yier.bubu.redis.storage.memory;

import java.util.List;
import java.util.Objects;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;
import yier.bubu.redis.storage.api.result.PayloadLengthSink;

public final class MaterializedCollectionScanWindow implements CollectionScanWindow {
    private static final long SOURCE_OBJECT_BYTES = 72L;
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long REFERENCE_BYTES = 8L;

    private final ScanCursorV2 nextCursor;
    private final int count;
    private final long retainedMemoryBytes;
    private byte[][] elements;

    public MaterializedCollectionScanWindow(ScanCursorV2 nextCursor, List<byte[]> elements) {
        this.nextCursor = Objects.requireNonNull(nextCursor, "nextCursor");
        Objects.requireNonNull(elements, "elements");
        this.elements = elements.toArray(byte[][]::new);
        this.count = this.elements.length;

        long retained = addSaturating(
                SOURCE_OBJECT_BYTES,
                alignedArrayBytes(this.elements.length, REFERENCE_BYTES)
        );
        for (byte[] element : this.elements) {
            if (element != null) {
                retained = addSaturating(retained, alignedArrayBytes(element.length, 1L));
            }
        }
        this.retainedMemoryBytes = retained;
    }

    @Override
    public ScanCursorV2 nextCursor() {
        return nextCursor;
    }

    @Override
    public int elementCount() {
        return count;
    }

    @Override
    public void visitElementLengths(PayloadLengthSink out) {
        Objects.requireNonNull(out, "out");
        byte[][] current = requireOpen();
        for (byte[] element : current) {
            out.payloadLength(element == null ? -1 : element.length);
        }
    }

    @Override
    public long retainedMemoryBytes() {
        return retainedMemoryBytes;
    }

    @Override
    public void emitTo(ByteValueSink out) {
        Objects.requireNonNull(out, "out");
        byte[][] current = requireOpen();
        for (byte[] element : current) {
            if (element == null) {
                out.nullValue();
            } else {
                out.value(element);
            }
        }
    }

    @Override
    public void close() {
        elements = null;
    }

    private byte[][] requireOpen() {
        byte[][] current = elements;
        if (current == null) {
            throw new IllegalStateException("collection scan window is closed");
        }
        return current;
    }

    private static long alignedArrayBytes(int length, long elementBytes) {
        long payload = multiplySaturating(Math.max(0, length), Math.max(0L, elementBytes));
        long bytes = addSaturating(ARRAY_HEADER_BYTES, payload);
        return bytes >= Long.MAX_VALUE - 7L ? Long.MAX_VALUE : (bytes + 7L) & ~7L;
    }

    private static long addSaturating(long left, long right) {
        return left < 0L || right < 0L || left > Long.MAX_VALUE - right
                ? Long.MAX_VALUE
                : left + right;
    }

    private static long multiplySaturating(long left, long right) {
        if (left == 0L || right == 0L) {
            return 0L;
        }
        return left > Long.MAX_VALUE / right ? Long.MAX_VALUE : left * right;
    }
}
