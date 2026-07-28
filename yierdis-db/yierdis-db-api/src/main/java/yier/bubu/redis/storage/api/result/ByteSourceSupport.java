package yier.bubu.redis.storage.api.result;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

abstract class ByteSourceSupport implements AutoCloseable {
    private final int count;
    private final long retainedMemoryBytes;
    private final String sourceName;
    private final AtomicBoolean closed = new AtomicBoolean();

    ByteSourceSupport(int count, String countName, long retainedMemoryBytes, String sourceName) {
        if (count < 0) {
            throw new IllegalArgumentException(countName + " must be non-negative");
        }
        if (retainedMemoryBytes < 0L) {
            throw new IllegalArgumentException("retainedMemoryBytes must be non-negative");
        }
        this.count = count;
        this.retainedMemoryBytes = retainedMemoryBytes;
        this.sourceName = sourceName;
    }

    final int count() {
        return count;
    }

    public final long retainedMemoryBytes() {
        return retainedMemoryBytes;
    }

    final void visitLengths(
            PayloadLengthSink out,
            int lengthsPerItem,
            Consumer<PayloadLengthSink> visitor
    ) {
        requireOpen();
        Objects.requireNonNull(out, "out");
        CountingLengthSink checked = new CountingLengthSink(
                out,
                Math.multiplyExact(count, lengthsPerItem)
        );
        visitor.accept(checked);
        checked.requireComplete();
    }

    final ByteValueSink emitterSink(ByteValueSink out) {
        requireOpen();
        return Objects.requireNonNull(out, "out");
    }

    @Override
    public final void close() {
        closed.set(true);
    }

    private void requireOpen() {
        if (closed.get()) {
            throw new IllegalStateException(sourceName + " is closed");
        }
    }

    private static final class CountingLengthSink implements PayloadLengthSink {
        private final PayloadLengthSink delegate;
        private final int expected;
        private int count;

        private CountingLengthSink(PayloadLengthSink delegate, int expected) {
            this.delegate = delegate;
            this.expected = expected;
        }

        @Override
        public void payloadLength(int length) {
            if (length < -1) {
                throw new IllegalArgumentException("payload length must be >= -1");
            }
            if (count >= expected) {
                throw new IllegalStateException("source emitted more lengths than declared");
            }
            count++;
            delegate.payloadLength(length);
        }

        private void requireComplete() {
            if (count != expected) {
                throw new IllegalStateException(
                        "source emitted " + count + " lengths but declared " + expected
                );
            }
        }
    }
}
