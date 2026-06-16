package yier.bubu.redis.app.bench.suite;

import java.util.List;
import java.util.Objects;

public record IterationResult(Kind kind, int index, List<SuiteMetric> metrics) {
    public enum Kind {
        WARMUP,
        REPEAT
    }

    public IterationResult {
        Objects.requireNonNull(kind, "kind");
        if (index < 0) {
            throw new IllegalArgumentException("index must be >= 0");
        }
        metrics = metrics == null ? List.of() : List.copyOf(metrics);
    }

    public static IterationResult warmup(int index, List<SuiteMetric> metrics) {
        return new IterationResult(Kind.WARMUP, index, metrics);
    }

    public static IterationResult repeat(int index, List<SuiteMetric> metrics) {
        return new IterationResult(Kind.REPEAT, index, metrics);
    }
}
