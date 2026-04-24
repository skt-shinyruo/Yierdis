package yier.bubu.redis.executor;

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
}
