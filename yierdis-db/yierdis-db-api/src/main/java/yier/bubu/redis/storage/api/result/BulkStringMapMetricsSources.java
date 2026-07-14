package yier.bubu.redis.storage.api.result;

import java.util.Objects;

/** BulkStringMapMetrics 的轻量重放实现。 */
public final class BulkStringMapMetricsSources {
    @FunctionalInterface
    public interface Emitter {
        void emitPairsTo(BulkStringSink out);
    }

    private BulkStringMapMetricsSources() {
    }

    public static BulkStringMapMetrics of(
            int pairCount,
            long encodedElementBytes,
            long retainedMemoryBytes,
            Emitter emitter
    ) {
        if (pairCount < 0 || encodedElementBytes < 0L || retainedMemoryBytes < 0L) {
            throw new IllegalArgumentException("measured map values must be non-negative");
        }
        return new Source(pairCount, encodedElementBytes, retainedMemoryBytes, emitter);
    }

    private static final class Source implements BulkStringMapMetrics {
        private final int pairCount;
        private final long encodedElementBytes;
        private final long retainedMemoryBytes;
        private final Emitter emitter;
        private boolean closed;

        private Source(int pairCount, long encodedElementBytes, long retainedMemoryBytes, Emitter emitter) {
            this.pairCount = pairCount;
            this.encodedElementBytes = encodedElementBytes;
            this.retainedMemoryBytes = retainedMemoryBytes;
            this.emitter = Objects.requireNonNull(emitter, "emitter");
        }

        @Override
        public int pairCount() {
            return pairCount;
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
        public void emitPairsTo(BulkStringSink out) {
            if (closed) {
                throw new IllegalStateException("measured map source is closed");
            }
            emitter.emitPairsTo(Objects.requireNonNull(out, "out"));
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
