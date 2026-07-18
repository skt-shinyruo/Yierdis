package yier.bubu.redis.app.bench.redis;

import org.HdrHistogram.Histogram;

public final class BenchmarkLatencyRecorder {
    private static final long LOWEST_DISCERNIBLE_MICROS = 10;
    private static final long HIGHEST_TRACKABLE_MICROS = 3_000_000;
    private static final int MIN_PRECISION = 0;
    private static final int MAX_PRECISION = 4;

    private final Histogram histogram;

    public BenchmarkLatencyRecorder(int precision) {
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new IllegalArgumentException("precision must be in range 0..4");
        }
        histogram = new Histogram(
                LOWEST_DISCERNIBLE_MICROS,
                HIGHEST_TRACKABLE_MICROS,
                precision
        );
    }

    public void recordMicros(long latencyMicros) {
        if (latencyMicros < 0) {
            throw new IllegalArgumentException("latencyMicros must be >= 0");
        }
        histogram.recordValue(Math.min(latencyMicros, HIGHEST_TRACKABLE_MICROS));
    }

    public Summary summary() {
        if (histogram.getTotalCount() == 0) {
            return Summary.empty();
        }
        return new Summary(
                histogram.getTotalCount(),
                histogram.getMean(),
                histogram.getMinValue(),
                histogram.getValueAtPercentile(50.0),
                histogram.getValueAtPercentile(95.0),
                histogram.getValueAtPercentile(99.0),
                histogram.getMaxValue()
        );
    }

    public record Summary(
            long count,
            double meanMicros,
            long minMicros,
            long p50Micros,
            long p95Micros,
            long p99Micros,
            long maxMicros
    ) {
        public Summary {
            requireNonNegative(count, "count");
            requireNonNegative(minMicros, "minMicros");
            requireNonNegative(p50Micros, "p50Micros");
            requireNonNegative(p95Micros, "p95Micros");
            requireNonNegative(p99Micros, "p99Micros");
            requireNonNegative(maxMicros, "maxMicros");
            if (!Double.isFinite(meanMicros) || meanMicros < 0.0) {
                throw new IllegalArgumentException("meanMicros must be finite and >= 0");
            }
            if (count == 0 && (Double.compare(meanMicros, 0.0) != 0
                    || minMicros != 0
                    || p50Micros != 0
                    || p95Micros != 0
                    || p99Micros != 0
                    || maxMicros != 0)) {
                throw new IllegalArgumentException("empty summaries must contain only zero metrics");
            }
            if (minMicros > p50Micros
                    || p50Micros > p95Micros
                    || p95Micros > p99Micros
                    || p99Micros > maxMicros) {
                throw new IllegalArgumentException(
                        "latency values must satisfy min <= p50 <= p95 <= p99 <= max"
                );
            }
            if (count > 0 && (meanMicros < minMicros || meanMicros > maxMicros)) {
                throw new IllegalArgumentException("meanMicros must be in minMicros..maxMicros");
            }
        }

        private static Summary empty() {
            return new Summary(0, 0.0, 0, 0, 0, 0, 0);
        }

        private static void requireNonNegative(long value, String name) {
            if (value < 0) {
                throw new IllegalArgumentException(name + " must be >= 0");
            }
        }
    }
}
