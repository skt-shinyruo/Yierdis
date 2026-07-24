package yier.bubu.redis.storage.api.result;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ByteMapSources {
    @FunctionalInterface
    public interface LengthVisitor {
        void visit(PayloadLengthSink out);
    }

    @FunctionalInterface
    public interface Emitter {
        void emit(ByteValueSink out);
    }

    private ByteMapSources() {
    }

    public static ByteMapSource empty() {
        return of(0, 0L, ignored -> { }, ignored -> { });
    }

    public static ByteMapSource of(
            int pairCount,
            long retainedMemoryBytes,
            LengthVisitor lengthVisitor,
            Emitter emitter
    ) {
        if (pairCount < 0) {
            throw new IllegalArgumentException("pairCount must be non-negative");
        }
        if (retainedMemoryBytes < 0L) {
            throw new IllegalArgumentException("retainedMemoryBytes must be non-negative");
        }
        return new Source(pairCount, retainedMemoryBytes, lengthVisitor, emitter);
    }

    private static final class Source implements ByteMapSource {
        private final int pairCount;
        private final long retainedMemoryBytes;
        private final LengthVisitor lengthVisitor;
        private final Emitter emitter;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Source(
                int pairCount,
                long retainedMemoryBytes,
                LengthVisitor lengthVisitor,
                Emitter emitter
        ) {
            this.pairCount = pairCount;
            this.retainedMemoryBytes = retainedMemoryBytes;
            this.lengthVisitor = Objects.requireNonNull(lengthVisitor, "lengthVisitor");
            this.emitter = Objects.requireNonNull(emitter, "emitter");
        }

        @Override
        public int pairCount() {
            return pairCount;
        }

        @Override
        public long retainedMemoryBytes() {
            return retainedMemoryBytes;
        }

        @Override
        public void visitPairLengths(PayloadLengthSink out) {
            requireOpen();
            Objects.requireNonNull(out, "out");
            CountingLengthSink checked = new CountingLengthSink(out, Math.multiplyExact(pairCount, 2));
            lengthVisitor.visit(checked);
            checked.requireComplete();
        }

        @Override
        public void emitPairsTo(ByteValueSink out) {
            requireOpen();
            emitter.emit(Objects.requireNonNull(out, "out"));
        }

        @Override
        public void close() {
            closed.set(true);
        }

        private void requireOpen() {
            if (closed.get()) {
                throw new IllegalStateException("byte map source is closed");
            }
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
