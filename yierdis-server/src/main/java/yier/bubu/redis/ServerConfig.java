package yier.bubu.redis;

final class ServerConfig {
    final int port;
    final long expirationCleanupIntervalMillis;
    final int ioThreads;
    final int executorQueueCapacity;
    final int backpressureHighWatermark;
    final int backpressureLowWatermark;
    final int executorMaxDrainCommands;
    final long executorDrainTimeLimitMillis;
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
            int backpressureHighWatermark,
            int backpressureLowWatermark,
            int executorMaxDrainCommands,
            long executorDrainTimeLimitMillis,
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
        if (ioThreads <= 0) {
            throw new IllegalArgumentException("ioThreads must be > 0");
        }
        if (executorQueueCapacity <= 0) {
            throw new IllegalArgumentException("executorQueueCapacity must be > 0");
        }
        this.ioThreads = ioThreads;
        this.executorQueueCapacity = executorQueueCapacity;
        if (backpressureHighWatermark <= 0) {
            throw new IllegalArgumentException("backpressureHighWatermark must be > 0");
        }
        if (backpressureLowWatermark < 0) {
            throw new IllegalArgumentException("backpressureLowWatermark must be >= 0");
        }
        if (backpressureLowWatermark >= backpressureHighWatermark) {
            throw new IllegalArgumentException("backpressureLowWatermark must be < backpressureHighWatermark");
        }
        if (executorMaxDrainCommands <= 0) {
            throw new IllegalArgumentException("executorMaxDrainCommands must be > 0");
        }
        if (executorDrainTimeLimitMillis <= 0) {
            throw new IllegalArgumentException("executorDrainTimeLimitMillis must be > 0");
        }
        this.backpressureHighWatermark = backpressureHighWatermark;
        this.backpressureLowWatermark = backpressureLowWatermark;
        this.executorMaxDrainCommands = executorMaxDrainCommands;
        this.executorDrainTimeLimitMillis = executorDrainTimeLimitMillis;
        this.offheapBackend = offheapBackend;
        this.offheapMaxBytes = offheapMaxBytes;
        this.maxmemoryBytes = maxmemoryBytes;
        this.maxmemoryPolicy = maxmemoryPolicy;
        this.maxmemorySamples = maxmemorySamples;
        if (evictionTimeLimitMillis <= 0) {
            throw new IllegalArgumentException("evictionTimeLimitMillis must be > 0");
        }
        if (expireCleanupTimeLimitMillis <= 0) {
            throw new IllegalArgumentException("expireCleanupTimeLimitMillis must be > 0");
        }
        this.evictionTimeLimitMillis = evictionTimeLimitMillis;
        this.expireCleanupTimeLimitMillis = expireCleanupTimeLimitMillis;
    }

    static ServerConfig fromArgs(String[] args) {
        int port = 6378;
        long cleanupIntervalMillis = 1000;
        int ioThreads = 1;
        int executorQueueCapacity = 1024;
        int backpressureHighWatermark = 256;
        int backpressureLowWatermark = 128;
        int executorMaxDrainCommands = 512;
        long executorDrainTimeLimitMillis = 2;
        String offheapBackend = "none";
        long offheapMaxBytes = 0;
        long maxmemoryBytes = 0;
        String maxmemoryPolicy = "noeviction";
        int maxmemorySamples = 5;
        long evictionTimeLimitMillis = 5;
        long expireCleanupTimeLimitMillis = 5;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("--port".equals(arg) && i + 1 < args.length) {
                port = Integer.parseInt(args[++i]);
                continue;
            }
            if ("--cleanupIntervalMillis".equals(arg) && i + 1 < args.length) {
                cleanupIntervalMillis = Long.parseLong(args[++i]);
                continue;
            }
            if ("--noCleanup".equals(arg)) {
                cleanupIntervalMillis = 0;
                continue;
            }
            if ("--ioThreads".equals(arg) && i + 1 < args.length) {
                ioThreads = Integer.parseInt(args[++i]);
                continue;
            }
            if ("--executorQueueCapacity".equals(arg) && i + 1 < args.length) {
                executorQueueCapacity = Integer.parseInt(args[++i]);
                continue;
            }
            if ("--backpressureHigh".equals(arg) && i + 1 < args.length) {
                backpressureHighWatermark = Integer.parseInt(args[++i]);
                continue;
            }
            if ("--backpressureLow".equals(arg) && i + 1 < args.length) {
                backpressureLowWatermark = Integer.parseInt(args[++i]);
                continue;
            }
            if ("--executorMaxDrain".equals(arg) && i + 1 < args.length) {
                executorMaxDrainCommands = Integer.parseInt(args[++i]);
                continue;
            }
            if ("--executorDrainMillis".equals(arg) && i + 1 < args.length) {
                executorDrainTimeLimitMillis = Long.parseLong(args[++i]);
                continue;
            }
            if ("--offheapBackend".equals(arg) && i + 1 < args.length) {
                offheapBackend = args[++i];
                continue;
            }
            if ("--offheapMaxBytes".equals(arg) && i + 1 < args.length) {
                offheapMaxBytes = Long.parseLong(args[++i]);
                continue;
            }
            if ("--maxmemoryBytes".equals(arg) && i + 1 < args.length) {
                maxmemoryBytes = Long.parseLong(args[++i]);
                continue;
            }
            if ("--maxmemoryPolicy".equals(arg) && i + 1 < args.length) {
                maxmemoryPolicy = args[++i];
                continue;
            }
            if ("--maxmemorySamples".equals(arg) && i + 1 < args.length) {
                maxmemorySamples = Integer.parseInt(args[++i]);
                continue;
            }
            if ("--evictionTimeLimitMillis".equals(arg) && i + 1 < args.length) {
                evictionTimeLimitMillis = Long.parseLong(args[++i]);
                continue;
            }
            if ("--expireCleanupTimeLimitMillis".equals(arg) && i + 1 < args.length) {
                expireCleanupTimeLimitMillis = Long.parseLong(args[++i]);
                continue;
            }
        }

        return new ServerConfig(
                port,
                cleanupIntervalMillis,
                ioThreads,
                executorQueueCapacity,
                backpressureHighWatermark,
                backpressureLowWatermark,
                executorMaxDrainCommands,
                executorDrainTimeLimitMillis,
                offheapBackend,
                offheapMaxBytes,
                maxmemoryBytes,
                maxmemoryPolicy,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis
        );
    }
}
