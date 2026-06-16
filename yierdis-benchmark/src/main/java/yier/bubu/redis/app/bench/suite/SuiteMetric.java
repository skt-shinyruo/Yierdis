package yier.bubu.redis.app.bench.suite;

public record SuiteMetric(String name, double value) {
    public SuiteMetric {
        if (name == null || !name.matches("[a-z0-9_]+")) {
            throw new IllegalArgumentException("metric name must be lowercase snake_case");
        }
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException("metric value must be finite");
        }
    }
}
