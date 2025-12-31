package yier.bubu.redis;

final class ServerConfig {
    final int port;
    final long expirationCleanupIntervalMillis;
    final String offheapBackend;
    final long offheapMaxBytes;
    final long maxmemoryBytes;
    final String maxmemoryPolicy;
    final int maxmemorySamples;

    private ServerConfig(
            int port,
            long expirationCleanupIntervalMillis,
            String offheapBackend,
            long offheapMaxBytes,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples
    ) {
        this.port = port;
        this.expirationCleanupIntervalMillis = expirationCleanupIntervalMillis;
        this.offheapBackend = offheapBackend;
        this.offheapMaxBytes = offheapMaxBytes;
        this.maxmemoryBytes = maxmemoryBytes;
        this.maxmemoryPolicy = maxmemoryPolicy;
        this.maxmemorySamples = maxmemorySamples;
    }

    static ServerConfig fromArgs(String[] args) {
        int port = 6378;
        long cleanupIntervalMillis = 1000;
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
                offheapBackend,
                offheapMaxBytes,
                maxmemoryBytes,
                maxmemoryPolicy,
                maxmemorySamples
        );
    }
}
