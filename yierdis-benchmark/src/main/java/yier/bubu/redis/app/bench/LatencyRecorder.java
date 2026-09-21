package yier.bubu.redis.app.bench;

import org.HdrHistogram.Histogram;

/**
 * HdrHistogram 包装：micros（10µs 分辨率、3s 上限）与 nanos（10s 上限）两种刻度，
 * 超上限值钳制进最高桶；precision 边界已在配置解析处验证。
 */
public final class LatencyRecorder {
    private final long recordFloor;
    private final long highestTrackable;
    private final Histogram histogram;

    private LatencyRecorder(
            long lowestDiscernible,
            long highestTrackable,
            long recordFloor,
            int precision
    ) {
        if (precision < 0 || precision > 4) {
            throw new IllegalArgumentException("precision must be in range 0..4");
        }
        this.recordFloor = recordFloor;
        this.highestTrackable = highestTrackable;
        this.histogram = new Histogram(lowestDiscernible, highestTrackable, precision);
    }

    public static LatencyRecorder micros(int precision) {
        return new LatencyRecorder(10, 3_000_000, 0, precision);
    }

    public static LatencyRecorder nanos(int precision) {
        return new LatencyRecorder(1, 10_000_000_000L, 1, precision);
    }

    public void record(long latency) {
        if (latency < 0) {
            throw new IllegalArgumentException("latency must be >= 0");
        }
        histogram.recordValue(Math.max(recordFloor, Math.min(latency, highestTrackable)));
    }

    public Summary summary() {
        if (histogram.getTotalCount() == 0) {
            return Summary.EMPTY;
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
            double mean,
            long min,
            long p50,
            long p95,
            long p99,
            long max
    ) {
        private static final Summary EMPTY = new Summary(0, 0.0, 0, 0, 0, 0, 0);
    }
}
