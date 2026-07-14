package yier.bubu.redis.app.bench.suite;

public record ThresholdPolicy(
        double qpsDropPercent,
        double latencyIncreasePercent,
        double productionHardeningMinimumMedianQpsRatio
) {
    public ThresholdPolicy {
        requireValidPercentage(qpsDropPercent, "qpsDropPercent");
        requireValidPercentage(latencyIncreasePercent, "latencyIncreasePercent");
        requireValidRatio(productionHardeningMinimumMedianQpsRatio, "productionHardeningMinimumMedianQpsRatio");
    }

    public ThresholdPolicy(double qpsDropPercent, double latencyIncreasePercent) {
        this(qpsDropPercent, latencyIncreasePercent, 0.90);
    }

    public static ThresholdPolicy defaults() {
        return new ThresholdPolicy(10.0, 15.0, 0.90);
    }

    private static void requireValidPercentage(double value, String name) {
        if (!Double.isFinite(value) || value < 0.0) {
            throw new IllegalArgumentException(name + " must be finite and >= 0");
        }
    }

    private static void requireValidRatio(double value, String name) {
        if (!Double.isFinite(value) || value <= 0.0 || value > 1.0) {
            throw new IllegalArgumentException(name + " must be finite and within (0, 1]");
        }
    }
}
