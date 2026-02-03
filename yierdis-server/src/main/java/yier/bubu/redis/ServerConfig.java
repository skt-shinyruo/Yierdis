package yier.bubu.redis;

import picocli.CommandLine;
import yier.bubu.redis.args.YierdisCliException;
import yier.bubu.redis.args.YierdisServerArgs;

import java.util.Objects;

final class ServerConfig {
    enum MaxmemoryScope {
        GLOBAL,
        PER_DB
    }

    final int port;
    final int databases;
    final long expirationCleanupIntervalMillis;
    final int ioThreads;
    final int executorQueueCapacity;
    final long executorQueueMaxBytes;
    final NettyCommandExecutor.SchedulingPolicy executorSchedulingPolicy;
    final long frameCompactionThresholdBytes;
    final double frameCompactionRatio;
    final int frameCompactionMaxCopyBytes;
    final int backpressureHighWatermark;
    final int backpressureLowWatermark;
    final long backpressureBytesHighWatermark;
    final long backpressureBytesLowWatermark;
    final int executorMaxDrainCommands;
    final long executorDrainTimeLimitMillis;
    final int transactionQueueMaxCommands;
    final long transactionQueueMaxBytes;
    final int protocolMaxBulkBytes;
    final int protocolMaxArgs;
    final int protocolMaxLineBytes;
    final String offheapBackend;
    final long offheapMaxBytes;
    final boolean offheapKeysEnabled;
    final long maxmemoryBytes;
    final MaxmemoryScope maxmemoryScope;
    final String maxmemoryPolicy;
    final int maxmemorySamples;
    final long evictionTimeLimitMillis;
    final long expireCleanupTimeLimitMillis;

    private ServerConfig(
            int port,
            int databases,
            long expirationCleanupIntervalMillis,
            int ioThreads,
            int executorQueueCapacity,
            long executorQueueMaxBytes,
            NettyCommandExecutor.SchedulingPolicy executorSchedulingPolicy,
            long frameCompactionThresholdBytes,
            double frameCompactionRatio,
            int frameCompactionMaxCopyBytes,
            int backpressureHighWatermark,
            int backpressureLowWatermark,
            long backpressureBytesHighWatermark,
            long backpressureBytesLowWatermark,
            int executorMaxDrainCommands,
            long executorDrainTimeLimitMillis,
            int transactionQueueMaxCommands,
            long transactionQueueMaxBytes,
            int protocolMaxBulkBytes,
            int protocolMaxArgs,
            int protocolMaxLineBytes,
            String offheapBackend,
            long offheapMaxBytes,
            boolean offheapKeysEnabled,
            long maxmemoryBytes,
            MaxmemoryScope maxmemoryScope,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        this.port = port;
        this.databases = databases;
        this.expirationCleanupIntervalMillis = expirationCleanupIntervalMillis;
        this.ioThreads = ioThreads;
        this.executorQueueCapacity = executorQueueCapacity;
        this.executorQueueMaxBytes = executorQueueMaxBytes;
        this.executorSchedulingPolicy = executorSchedulingPolicy;
        this.frameCompactionThresholdBytes = frameCompactionThresholdBytes;
        this.frameCompactionRatio = frameCompactionRatio;
        this.frameCompactionMaxCopyBytes = frameCompactionMaxCopyBytes;
        this.backpressureHighWatermark = backpressureHighWatermark;
        this.backpressureLowWatermark = backpressureLowWatermark;
        this.backpressureBytesHighWatermark = backpressureBytesHighWatermark;
        this.backpressureBytesLowWatermark = backpressureBytesLowWatermark;
        this.executorMaxDrainCommands = executorMaxDrainCommands;
        this.executorDrainTimeLimitMillis = executorDrainTimeLimitMillis;
        this.transactionQueueMaxCommands = transactionQueueMaxCommands;
        this.transactionQueueMaxBytes = transactionQueueMaxBytes;
        this.protocolMaxBulkBytes = protocolMaxBulkBytes;
        this.protocolMaxArgs = protocolMaxArgs;
        this.protocolMaxLineBytes = protocolMaxLineBytes;
        this.offheapBackend = offheapBackend;
        this.offheapMaxBytes = offheapMaxBytes;
        this.offheapKeysEnabled = offheapKeysEnabled;
        this.maxmemoryBytes = maxmemoryBytes;
        this.maxmemoryScope = Objects.requireNonNull(maxmemoryScope, "maxmemoryScope");
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
            throw YierdisCliException.usageError(e.getMessage(), e);
        }

        if (parsed.help) {
            cmd.usage(System.out);
            return null;
        }

        try {
            parsed.normalizeAndValidate();
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            cmd.usage(System.err);
            throw YierdisCliException.usageError(e.getMessage(), e);
        }

        NettyCommandExecutor.SchedulingPolicy schedulingPolicy =
                "global".equals(parsed.executorSchedulingPolicy)
                        ? NettyCommandExecutor.SchedulingPolicy.GLOBAL
                        : NettyCommandExecutor.SchedulingPolicy.FAIR;

        MaxmemoryScope maxmemoryScope =
                "per-db".equals(parsed.maxmemoryScope) ? MaxmemoryScope.PER_DB : MaxmemoryScope.GLOBAL;

        return new ServerConfig(
                parsed.port,
                parsed.databases,
                parsed.cleanupIntervalMillis,
                parsed.ioThreads,
                parsed.executorQueueCapacity,
                parsed.executorQueueMaxBytes,
                schedulingPolicy,
                parsed.frameCompactionThresholdBytes,
                parsed.frameCompactionRatio,
                parsed.frameCompactionMaxCopyBytes,
                parsed.backpressureHighWatermark,
                parsed.backpressureLowWatermark,
                parsed.backpressureBytesHighWatermark,
                parsed.backpressureBytesLowWatermark,
                parsed.executorMaxDrainCommands,
                parsed.executorDrainTimeLimitMillis,
                parsed.transactionQueueMaxCommands,
                parsed.transactionQueueMaxBytes,
                parsed.protocolMaxBulkBytes,
                parsed.protocolMaxArgs,
                parsed.protocolMaxLineBytes,
                parsed.offheapBackend,
                parsed.offheapMaxBytes,
                parsed.offheapKeysEnabled,
                parsed.maxmemoryBytes,
                maxmemoryScope,
                parsed.maxmemoryPolicy,
                parsed.maxmemorySamples,
                parsed.evictionTimeLimitMillis,
                parsed.expireCleanupTimeLimitMillis
        );
    }
}
