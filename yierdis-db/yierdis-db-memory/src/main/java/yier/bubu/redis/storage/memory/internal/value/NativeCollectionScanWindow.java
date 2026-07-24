package yier.bubu.redis.storage.memory.internal.value;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;
import yier.bubu.redis.storage.api.result.PayloadLengthSink;

/**
 * HT 编码集合的一次有界 scan 结果。窗口只 pin 已发现元素的 native handle，不复制 payload。
 */
final class NativeCollectionScanWindow implements CollectionScanWindow {
    private static final int MAX_MATCHES_PER_CALL = 1_024;
    private static final long MIN_SLOT_BUDGET = 64L;
    private static final long SLOT_MULTIPLIER = 10L;
    private static final long WINDOW_FIXED_HEAP_BYTES = 96L;
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long REFERENCE_BYTES = 8L;
    private static final long ELEMENT_HEAP_BYTES = 32L;

    private final ScanCursorV2 nextCursor;
    private final int count;
    private final long retainedMemoryBytes;
    private StableMemoryBackend allocator;
    private Element[] elements;

    private NativeCollectionScanWindow(
            StableMemoryBackend allocator,
            ScanCursorV2 nextCursor,
            Element[] elements,
            long retainedMemoryBytes
    ) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.nextCursor = Objects.requireNonNull(nextCursor, "nextCursor");
        this.elements = Objects.requireNonNull(elements, "elements");
        this.count = elements.length;
        this.retainedMemoryBytes = retainedMemoryBytes;
    }

    static Builder builder(StableMemoryBackend allocator, int expectedElements) {
        return new Builder(allocator, expectedElements);
    }

    static int boundedMatchCount(int requestedCount) {
        if (requestedCount <= 0) {
            throw new IllegalArgumentException("count must be > 0");
        }
        // Redis 将 COUNT 定义为 hint；内部上限防止单条命令按客户端给出的极大 COUNT 长时间占用 owner thread。
        return Math.min(requestedCount, MAX_MATCHES_PER_CALL);
    }

    static long slotBudget(int boundedMatchCount) {
        if (boundedMatchCount <= 0 || boundedMatchCount > MAX_MATCHES_PER_CALL) {
            throw new IllegalArgumentException("boundedMatchCount is out of range");
        }
        return Math.max(MIN_SLOT_BUDGET, (long) boundedMatchCount * SLOT_MULTIPLIER);
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
        Element[] current = requireOpen();
        for (Element element : current) {
            out.payloadLength(element.payloadLength());
        }
    }

    @Override
    public long retainedMemoryBytes() {
        return retainedMemoryBytes;
    }

    @Override
    public void emitTo(ByteValueSink out) {
        Objects.requireNonNull(out, "out");
        Element[] current = requireOpen();
        StableMemoryBackend currentAllocator = allocator;
        for (Element element : current) {
            element.emit(currentAllocator, out);
        }
    }

    @Override
    public void close() {
        Element[] current = elements;
        StableMemoryBackend currentAllocator = allocator;
        if (current == null || currentAllocator == null) {
            return;
        }
        elements = null;
        allocator = null;

        Throwable failure = null;
        for (Element element : current) {
            if (!(element instanceof NativeElement nativeElement)) {
                continue;
            }
            try {
                currentAllocator.unpin(nativeElement.handle());
            } catch (RuntimeException | Error next) {
                if (failure == null) {
                    failure = next;
                } else {
                    failure.addSuppressed(next);
                }
            }
        }
        rethrow(failure);
    }

    private Element[] requireOpen() {
        Element[] current = elements;
        if (current == null || allocator == null) {
            throw new IllegalStateException("collection scan window is closed");
        }
        return current;
    }

    private sealed interface Element permits NativeElement, ByteArrayElement, LongElement, NullElement {
        int payloadLength();

        void emit(StableMemoryBackend allocator, ByteValueSink out);
    }

    private record NativeElement(NativeHandle handle, int length) implements Element {
        private NativeElement {
            Objects.requireNonNull(handle, "handle");
            if (length < 0) {
                throw new IllegalArgumentException("length must be >= 0");
            }
        }

        @Override
        public int payloadLength() {
            return length;
        }

        @Override
        public void emit(StableMemoryBackend allocator, ByteValueSink out) {
            out.value(NativeBytesSlice.retained(allocator, handle, 0, length));
        }
    }

    private record ByteArrayElement(byte[] bytes) implements Element {
        private ByteArrayElement {
            Objects.requireNonNull(bytes, "bytes");
        }

        @Override
        public int payloadLength() {
            return bytes.length;
        }

        @Override
        public void emit(StableMemoryBackend allocator, ByteValueSink out) {
            out.value(bytes);
        }
    }

    private record LongElement(long value) implements Element {
        @Override
        public int payloadLength() {
            return SemanticResultSupport.signedLongAsciiLength(value);
        }

        @Override
        public void emit(StableMemoryBackend allocator, ByteValueSink out) {
            out.longAscii(value);
        }
    }

    private enum NullElement implements Element {
        INSTANCE;

        @Override
        public int payloadLength() {
            return -1;
        }

        @Override
        public void emit(StableMemoryBackend allocator, ByteValueSink out) {
            out.nullValue();
        }
    }

    static final class Builder implements AutoCloseable {
        private final StableMemoryBackend allocator;
        private final List<Element> elements;
        private long retainedElementBytes;
        private boolean transferred;

        private Builder(StableMemoryBackend allocator, int expectedElements) {
            this.allocator = Objects.requireNonNull(allocator, "allocator");
            if (expectedElements < 0) {
                throw new IllegalArgumentException("expectedElements must be >= 0");
            }
            this.elements = new ArrayList<>(expectedElements);
        }

        void addNative(NativeHandle handle, int length) {
            Objects.requireNonNull(handle, "handle");
            if (length < 0) {
                throw new IllegalArgumentException("length must be >= 0");
            }
            allocator.pin(handle);
            boolean added = false;
            try {
                long nativeRetainedBytes;
                try (NativeObjectView view = allocator.resolvePinned(handle, NativeAccessMode.READ_ONLY)) {
                    if (length > view.size()) {
                        throw new IndexOutOfBoundsException(
                                "length=" + length + ", native object size=" + view.size()
                        );
                    }
                    nativeRetainedBytes = view.capacity();
                }
                elements.add(new NativeElement(handle, length));
                // pin 会让已从集合删除的 allocation 延迟回收，因此 reply 额度必须同时覆盖 native payload。
                recordElement(addSaturating(ELEMENT_HEAP_BYTES, nativeRetainedBytes));
                added = true;
            } finally {
                if (!added) {
                    allocator.unpin(handle);
                }
            }
        }

        void addBytes(byte[] bytes) {
            Objects.requireNonNull(bytes, "bytes");
            elements.add(new ByteArrayElement(bytes));
            recordElement(addSaturating(ELEMENT_HEAP_BYTES, alignedArrayBytes(bytes.length, 1L)));
        }

        void addLong(long value) {
            elements.add(new LongElement(value));
            recordElement(ELEMENT_HEAP_BYTES);
        }

        void addNull() {
            elements.add(NullElement.INSTANCE);
            recordElement(0L);
        }

        CollectionScanWindow build(ScanCursorV2 nextCursor) {
            if (transferred) {
                throw new IllegalStateException("collection scan builder already transferred");
            }
            Element[] snapshot = elements.toArray(Element[]::new);
            long retained = addSaturating(
                    WINDOW_FIXED_HEAP_BYTES,
                    addSaturating(alignedArrayBytes(snapshot.length, REFERENCE_BYTES), retainedElementBytes)
            );
            NativeCollectionScanWindow window = new NativeCollectionScanWindow(
                    allocator,
                    nextCursor,
                    snapshot,
                    retained
            );
            transferred = true;
            elements.clear();
            return window;
        }

        @Override
        public void close() {
            if (transferred) {
                return;
            }
            Throwable failure = null;
            for (Element element : elements) {
                if (!(element instanceof NativeElement nativeElement)) {
                    continue;
                }
                try {
                    allocator.unpin(nativeElement.handle());
                } catch (RuntimeException | Error next) {
                    if (failure == null) {
                        failure = next;
                    } else {
                        failure.addSuppressed(next);
                    }
                }
            }
            elements.clear();
            rethrow(failure);
        }

        private void recordElement(long retainedBytes) {
            retainedElementBytes = addSaturating(retainedElementBytes, retainedBytes);
        }
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

    private static void rethrow(Throwable failure) {
        if (failure == null) {
            return;
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new AssertionError("unexpected collection scan close failure", failure);
    }
}
