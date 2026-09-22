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
        // Value ranges are validated once at the CLI trust boundary (YierdisServerRuntimeConfig);
        // this record only carries the validated values into the executor.
        Objects.requireNonNull(schedulingPolicy, "schedulingPolicy");
    }
}
