package yier.bubu.redis.storage.api.result;

import java.util.Objects;

/** MeasuredBulkStringSequence 的轻量重放实现。 */
public final class MeasuredBulkStringSequences {
    @FunctionalInterface
    public interface Emitter {
        void emitTo(BulkStringSink out);
    }

    private MeasuredBulkStringSequences() {
    }

    public static MeasuredBulkStringSequence of(
            int count,
            long encodedElementBytes,
            long retainedMemoryBytes,
            Emitter emitter
    ) {
        if (count < 0 || encodedElementBytes < 0L || retainedMemoryBytes < 0L) {
            throw new IllegalArgumentException("measured sequence values must be non-negative");
        }
        return new Source(count, encodedElementBytes, retainedMemoryBytes, emitter);
    }

    private static final class Source implements MeasuredBulkStringSequence {
        private final int count;
        private final long encodedElementBytes;
        private final long retainedMemoryBytes;
        private final Emitter emitter;
        private boolean closed;

        private Source(int count, long encodedElementBytes, long retainedMemoryBytes, Emitter emitter) {
            this.count = count;
            this.encodedElementBytes = encodedElementBytes;
            this.retainedMemoryBytes = retainedMemoryBytes;
            this.emitter = Objects.requireNonNull(emitter, "emitter");
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
            if (closed) {
                throw new IllegalStateException("measured sequence is closed");
            }
            emitter.emitTo(Objects.requireNonNull(out, "out"));
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
