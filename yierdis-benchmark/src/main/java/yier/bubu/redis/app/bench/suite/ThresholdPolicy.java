package yier.bubu.redis.app.bench.suite;

public record ThresholdPolicy(double qpsDropPercent, double latencyIncreasePercent) {
    public static ThresholdPolicy defaults() {
        return new ThresholdPolicy(10.0, 15.0);
    }
}
