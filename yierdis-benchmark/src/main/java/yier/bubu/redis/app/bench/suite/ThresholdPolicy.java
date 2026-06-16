package yier.bubu.redis.app.bench.suite;

public record ThresholdPolicy(double qpsDropPercent, double latencyIncreasePercent) {
    public ThresholdPolicy {
        requireValidPercentage(qpsDropPercent, "qpsDropPercent");
        requireValidPercentage(latencyIncreasePercent, "latencyIncreasePercent");
    }

    public static ThresholdPolicy defaults() {
        return new ThresholdPolicy(10.0, 15.0);
    }

    private static void requireValidPercentage(double value, String name) {
        if (!Double.isFinite(value) || value < 0.0) {
            throw new IllegalArgumentException(name + " must be finite and >= 0");
        }
    }
}
