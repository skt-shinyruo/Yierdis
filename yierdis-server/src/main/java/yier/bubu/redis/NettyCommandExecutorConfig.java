package yier.bubu.redis;

// NettyCommandExecutorConfig：收敛执行器的构造参数（队列/背压/drain/调度策略），避免 call site 参数爆炸。

import java.util.Objects;

/**
 * Immutable configuration for {@link NettyCommandExecutor}.
 */
record NettyCommandExecutorConfig(
        int queueCapacity,
        long queueMaxBytes,
        int backpressureHighWatermark,
        int backpressureLowWatermark,
        long backpressureBytesHighWatermark,
        long backpressureBytesLowWatermark,
        int maxDrainCommands,
        long drainTimeLimitMillis,
        NettyCommandExecutor.SchedulingPolicy schedulingPolicy
) {
    static NettyCommandExecutorConfig from(ServerConfig config) {
        Objects.requireNonNull(config, "config");
        return new NettyCommandExecutorConfig(
                config.executorQueueCapacity,
                config.executorQueueMaxBytes,
                config.backpressureHighWatermark,
                config.backpressureLowWatermark,
                config.backpressureBytesHighWatermark,
                config.backpressureBytesLowWatermark,
                config.executorMaxDrainCommands,
                config.executorDrainTimeLimitMillis,
                config.executorSchedulingPolicy
        );
    }
}

