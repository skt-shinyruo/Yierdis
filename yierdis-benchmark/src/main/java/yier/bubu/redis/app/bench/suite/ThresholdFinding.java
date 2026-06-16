package yier.bubu.redis.app.bench.suite;

public record ThresholdFinding(Level level, String scenarioId, String metric, String message) {
    public enum Level {
        WARNING,
        CRITICAL
    }
}
