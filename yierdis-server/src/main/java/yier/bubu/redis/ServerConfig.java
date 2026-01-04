package yier.bubu.redis;

final class ServerConfig {
    final int port;
    final long expirationCleanupIntervalMillis;
    final int ioThreads;
    final int executorQueueCapacity;
    final String offheapBackend;
    final long offheapMaxBytes;
    final long maxmemoryBytes;
    final String maxmemoryPolicy;
    final int maxmemorySamples;

    private ServerConfig(
            int port,
            long expirationCleanupIntervalMillis,
            int ioThreads,
            int executorQueueCapacity,
            String offheapBackend,
            long offheapMaxBytes,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples
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
        this.offheapBackend = offheapBackend;
        this.offheapMaxBytes = offheapMaxBytes;
        this.maxmemoryBytes = maxmemoryBytes;
        this.maxmemoryPolicy = maxmemoryPolicy;
        this.maxmemorySamples = maxmemorySamples;
    }

    static ServerConfig fromArgs(String[] args) {
        int port = 6378;
        long cleanupIntervalMillis = 1000;
        int ioThreads = 1;
        int executorQueueCapacity = 1024;
        String offheapBackend = "none";
        long offheapMaxBytes = 0;
        long maxmemoryBytes = 0;
        String maxmemoryPolicy = "noeviction";
        int maxmemorySamples = 5;

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
        }

        return new ServerConfig(
                port,
                cleanupIntervalMillis,
                ioThreads,
                executorQueueCapacity,
                offheapBackend,
                offheapMaxBytes,
                maxmemoryBytes,
                maxmemoryPolicy,
                maxmemorySamples
        );
    }
}
