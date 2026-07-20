package yier.bubu.redis.storage.memory;

import java.util.List;
import java.util.Objects;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.result.BulkStringMetrics;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;

public final class MaterializedCollectionScanWindow implements CollectionScanWindow {
    private static final long SOURCE_OBJECT_BYTES = 72L;
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long REFERENCE_BYTES = 8L;

    private final ScanCursorV2 nextCursor;
    private final int count;
    private final long encodedElementBytes;
    private final long retainedMemoryBytes;
    private byte[][] elements;

    public MaterializedCollectionScanWindow(ScanCursorV2 nextCursor, List<byte[]> elements) {
        this.nextCursor = Objects.requireNonNull(nextCursor, "nextCursor");
        Objects.requireNonNull(elements, "elements");
        this.elements = elements.toArray(byte[][]::new);
        this.count = this.elements.length;

        BulkStringMetrics metrics = new BulkStringMetrics();
        long retained = addSaturating(
                SOURCE_OBJECT_BYTES,
                alignedArrayBytes(this.elements.length, REFERENCE_BYTES)
        );
        for (byte[] element : this.elements) {
            metrics.bulkString(element);
            if (element != null) {
                retained = addSaturating(retained, alignedArrayBytes(element.length, 1L));
            }
        }
        this.encodedElementBytes = metrics.encodedElementBytes();
        this.retainedMemoryBytes = retained;
    }

    @Override
    public ScanCursorV2 nextCursor() {
        return nextCursor;
    }

    @Override
    public int count() {
        return count;
    }

    @Override
    public long encodedElementBytes() {
        return encodedElementBytes;
    }

    @Override
    public long retainedMemoryBytes() {
        return retainedMemoryBytes;
    }

    @Override
    public void emitTo(BulkStringSink out) {
        Objects.requireNonNull(out, "out");
        byte[][] current = elements;
        if (current == null) {
            throw new IllegalStateException("collection scan window is closed");
        }
        for (byte[] element : current) {
            out.bulkString(element);
        }
    }

    @Override
    public void close() {
        elements = null;
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
