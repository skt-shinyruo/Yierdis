package yier.bubu.redis;

import picocli.CommandLine;
import yier.bubu.redis.args.YierdisServerArgs;

final class ServerConfig {
    final int port;
    final long expirationCleanupIntervalMillis;
    final int ioThreads;
    final int executorQueueCapacity;
    final long executorQueueMaxBytes;
    final int backpressureHighWatermark;
    final int backpressureLowWatermark;
    final long backpressureBytesHighWatermark;
    final long backpressureBytesLowWatermark;
    final int executorMaxDrainCommands;
    final long executorDrainTimeLimitMillis;
    final int protocolMaxBulkBytes;
    final int protocolMaxArgs;
    final int protocolMaxLineBytes;
    final String offheapBackend;
    final long offheapMaxBytes;
    final long maxmemoryBytes;
    final String maxmemoryPolicy;
    final int maxmemorySamples;
    final long evictionTimeLimitMillis;
    final long expireCleanupTimeLimitMillis;

    private ServerConfig(
            int port,
            long expirationCleanupIntervalMillis,
            int ioThreads,
            int executorQueueCapacity,
            long executorQueueMaxBytes,
            int backpressureHighWatermark,
            int backpressureLowWatermark,
            long backpressureBytesHighWatermark,
            long backpressureBytesLowWatermark,
            int executorMaxDrainCommands,
            long executorDrainTimeLimitMillis,
            int protocolMaxBulkBytes,
            int protocolMaxArgs,
            int protocolMaxLineBytes,
            String offheapBackend,
            long offheapMaxBytes,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        this.port = port;
        this.expirationCleanupIntervalMillis = expirationCleanupIntervalMillis;
        this.ioThreads = ioThreads;
        this.executorQueueCapacity = executorQueueCapacity;
        this.executorQueueMaxBytes = executorQueueMaxBytes;
        this.backpressureHighWatermark = backpressureHighWatermark;
        this.backpressureLowWatermark = backpressureLowWatermark;
        this.backpressureBytesHighWatermark = backpressureBytesHighWatermark;
        this.backpressureBytesLowWatermark = backpressureBytesLowWatermark;
        this.executorMaxDrainCommands = executorMaxDrainCommands;
        this.executorDrainTimeLimitMillis = executorDrainTimeLimitMillis;
        this.protocolMaxBulkBytes = protocolMaxBulkBytes;
        this.protocolMaxArgs = protocolMaxArgs;
        this.protocolMaxLineBytes = protocolMaxLineBytes;
        this.offheapBackend = offheapBackend;
        this.offheapMaxBytes = offheapMaxBytes;
        this.maxmemoryBytes = maxmemoryBytes;
        this.maxmemoryPolicy = maxmemoryPolicy;
        this.maxmemorySamples = maxmemorySamples;
        this.evictionTimeLimitMillis = evictionTimeLimitMillis;
        this.expireCleanupTimeLimitMillis = expireCleanupTimeLimitMillis;
    }

    static ServerConfig fromArgs(String[] args) {
        YierdisServerArgs parsed = new YierdisServerArgs();
        CommandLine cmd = new CommandLine(parsed);
        try {
            cmd.parseArgs(args);
        } catch (CommandLine.ParameterException e) {
            System.err.println(e.getMessage());
            cmd.usage(System.err);
            throw new IllegalArgumentException(e.getMessage(), e);
        }

        if (parsed.help) {
            cmd.usage(System.out);
            return null;
        }

        parsed.normalizeAndValidate();

        return new ServerConfig(
                parsed.port,
                parsed.cleanupIntervalMillis,
                parsed.ioThreads,
                parsed.executorQueueCapacity,
                parsed.executorQueueMaxBytes,
                parsed.backpressureHighWatermark,
                parsed.backpressureLowWatermark,
                parsed.backpressureBytesHighWatermark,
                parsed.backpressureBytesLowWatermark,
                parsed.executorMaxDrainCommands,
                parsed.executorDrainTimeLimitMillis,
                parsed.protocolMaxBulkBytes,
                parsed.protocolMaxArgs,
                parsed.protocolMaxLineBytes,
                parsed.offheapBackend,
                parsed.offheapMaxBytes,
                parsed.maxmemoryBytes,
                parsed.maxmemoryPolicy,
                parsed.maxmemorySamples,
                parsed.evictionTimeLimitMillis,
                parsed.expireCleanupTimeLimitMillis
        );
    }
}
