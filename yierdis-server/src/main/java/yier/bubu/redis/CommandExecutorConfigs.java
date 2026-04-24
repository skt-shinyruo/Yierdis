package yier.bubu.redis;

import yier.bubu.redis.args.YierdisServerRuntimeConfig;
import yier.bubu.redis.executor.CommandExecutorConfig;
import yier.bubu.redis.executor.SchedulingPolicy;

import java.util.Objects;

final class CommandExecutorConfigs {
    private CommandExecutorConfigs() {
    }

    static CommandExecutorConfig from(YierdisServerRuntimeConfig config) {
        Objects.requireNonNull(config, "config");
        return new CommandExecutorConfig(
                config.executorQueueCapacity(),
                config.executorQueueMaxBytes(),
                config.backpressureHighWatermark(),
                config.backpressureLowWatermark(),
                config.backpressureBytesHighWatermark(),
                config.backpressureBytesLowWatermark(),
                config.executorMaxDrainCommands(),
                config.executorDrainTimeLimitMillis(),
                switch (config.executorSchedulingPolicy()) {
                    case GLOBAL -> SchedulingPolicy.GLOBAL;
                    case FAIR -> SchedulingPolicy.FAIR;
                }
        );
    }
}
