package yier.bubu.redis.execution.executor;

import java.util.Objects;

public record CommandExecutorConfig(
        int queueCapacity,
        long queueMaxBytes,
        int backpressureHighWatermark,
        int backpressureLowWatermark,
        long backpressureBytesHighWatermark,
        long backpressureBytesLowWatermark,
        int maxDrainCommands,
        long drainTimeLimitMillis,
        SchedulingPolicy schedulingPolicy
) {
    public CommandExecutorConfig {
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be > 0");
        }
        if (queueMaxBytes < 0) {
            throw new IllegalArgumentException("queueMaxBytes must be >= 0");
        }
        if (backpressureHighWatermark <= 0) {
            throw new IllegalArgumentException("backpressureHighWatermark must be > 0");
        }
        if (backpressureLowWatermark < 0) {
            throw new IllegalArgumentException("backpressureLowWatermark must be >= 0");
        }
        if (backpressureLowWatermark >= backpressureHighWatermark) {
            throw new IllegalArgumentException("backpressureLowWatermark must be < backpressureHighWatermark");
        }
        if (backpressureBytesHighWatermark < 0) {
            throw new IllegalArgumentException("backpressureBytesHighWatermark must be >= 0");
        }
        if (backpressureBytesLowWatermark < 0) {
            throw new IllegalArgumentException("backpressureBytesLowWatermark must be >= 0");
        }
        if (backpressureBytesHighWatermark == 0 && backpressureBytesLowWatermark != 0) {
            throw new IllegalArgumentException("backpressureBytesLowWatermark must be 0 when backpressureBytesHighWatermark is disabled");
        }
        if (backpressureBytesHighWatermark > 0
                && backpressureBytesLowWatermark >= backpressureBytesHighWatermark) {
            throw new IllegalArgumentException("backpressureBytesLowWatermark must be < backpressureBytesHighWatermark");
        }
        if (maxDrainCommands <= 0) {
            throw new IllegalArgumentException("maxDrainCommands must be > 0");
        }
        if (drainTimeLimitMillis <= 0) {
            throw new IllegalArgumentException("drainTimeLimitMillis must be > 0");
        }
        Objects.requireNonNull(schedulingPolicy, "schedulingPolicy");
    }
}
