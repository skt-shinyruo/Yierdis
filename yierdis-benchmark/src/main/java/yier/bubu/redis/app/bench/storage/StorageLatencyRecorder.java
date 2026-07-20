package yier.bubu.redis.app.bench.storage;

import org.HdrHistogram.Histogram;

public final class StorageLatencyRecorder {
    private static final long HIGHEST_TRACKABLE_NANOS = 10_000_000_000L;

    private final Histogram histogram;

    StorageLatencyRecorder(int precision) {
        if (precision < 0 || precision > 4) {
            throw new IllegalArgumentException("precision must be in range 0..4");
        }
        histogram = new Histogram(1L, HIGHEST_TRACKABLE_NANOS, precision);
    }

    void recordNanos(long latencyNanos) {
        long bounded = Math.max(1L, Math.min(latencyNanos, HIGHEST_TRACKABLE_NANOS));
        histogram.recordValue(bounded);
    }

    Summary summary() {
        if (histogram.getTotalCount() == 0L) {
            return new Summary(0L, 0.0, 0L, 0L, 0L);
        }
        return new Summary(
                histogram.getTotalCount(),
                histogram.getMean(),
                histogram.getValueAtPercentile(50.0),
                histogram.getValueAtPercentile(99.0),
                histogram.getMaxValue()
        );
    }

    public record Summary(long count, double meanNanos, long p50Nanos, long p99Nanos, long maxNanos) {
        public Summary {
            if (count < 0L || !Double.isFinite(meanNanos) || meanNanos < 0.0
                    || p50Nanos < 0L || p99Nanos < p50Nanos || maxNanos < p99Nanos) {
                throw new IllegalArgumentException("invalid latency summary");
            }
        }
    }
}
