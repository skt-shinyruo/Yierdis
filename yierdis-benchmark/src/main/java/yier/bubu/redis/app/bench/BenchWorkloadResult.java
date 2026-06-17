package yier.bubu.redis.app.bench;

import yier.bubu.redis.app.bench.suite.SuiteMetric;

import java.util.ArrayList;
import java.util.List;

public record BenchWorkloadResult(
        long ops,
        long errors,
        double seconds,
        double qps,
        double p50Millis,
        double p95Millis,
        double p99Millis
) {
    public BenchWorkloadResult {
        if (ops < 0) {
            throw new IllegalArgumentException("ops must be >= 0");
        }
        if (errors < 0) {
            throw new IllegalArgumentException("errors must be >= 0");
        }
        requireFiniteNonnegative(seconds, "seconds");
        requireFiniteNonnegative(qps, "qps");
        requireLatencyMetric(p50Millis, "p50Millis");
        requireLatencyMetric(p95Millis, "p95Millis");
        requireLatencyMetric(p99Millis, "p99Millis");
        boolean hasP50 = !Double.isNaN(p50Millis);
        boolean hasP95 = !Double.isNaN(p95Millis);
        boolean hasP99 = !Double.isNaN(p99Millis);
        if (hasP50 != hasP95 || hasP50 != hasP99) {
            throw new IllegalArgumentException("latency millis values must be all present or all NaN");
        }
    }

    public List<SuiteMetric> toMetrics() {
        List<SuiteMetric> metrics = new ArrayList<>();
        metrics.add(new SuiteMetric("ops", ops));
        metrics.add(new SuiteMetric("errors", errors));
        metrics.add(new SuiteMetric("seconds", seconds));
        metrics.add(new SuiteMetric("qps", qps));
        if (!Double.isNaN(p50Millis)) {
            metrics.add(new SuiteMetric("p50_ms", p50Millis));
            metrics.add(new SuiteMetric("p95_ms", p95Millis));
            metrics.add(new SuiteMetric("p99_ms", p99Millis));
        }
        return List.copyOf(metrics);
    }

    private static void requireFiniteNonnegative(double value, String name) {
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException(name + " must be finite");
        }
        if (value < 0.0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }

    private static void requireLatencyMetric(double value, String name) {
        if (Double.isNaN(value)) {
            return;
        }
        requireFiniteNonnegative(value, name);
    }
}
