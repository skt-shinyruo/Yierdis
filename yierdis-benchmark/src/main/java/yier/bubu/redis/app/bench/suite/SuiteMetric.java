package yier.bubu.redis.app.bench.suite;

public record SuiteMetric(String name, double value) {
    public SuiteMetric {
        requireValidName(name);
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException("metric value must be finite");
        }
        if (value < 0.0) {
            throw new IllegalArgumentException("metric value must be >= 0");
        }
    }

    static void requireValidName(String name) {
        if (name == null || !name.matches("[a-z0-9]+(?:_[a-z0-9]+)*")) {
            throw new IllegalArgumentException("metric name must be lowercase snake_case");
        }
    }
}
