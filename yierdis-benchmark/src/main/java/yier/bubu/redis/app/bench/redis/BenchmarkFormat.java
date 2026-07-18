package yier.bubu.redis.app.bench.redis;

import java.util.Locale;

public enum BenchmarkFormat {
    HUMAN,
    QUIET,
    CSV;

    public static BenchmarkFormat parse(String value) {
        if (value == null) {
            throw new IllegalArgumentException("format must be one of: human, quiet, csv");
        }
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "human" -> HUMAN;
            case "quiet" -> QUIET;
            case "csv" -> CSV;
            default -> throw new IllegalArgumentException("format must be one of: human, quiet, csv");
        };
    }
}
