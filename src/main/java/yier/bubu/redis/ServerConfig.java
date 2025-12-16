package yier.bubu.redis;

final class ServerConfig {
    final int port;
    final long expirationCleanupIntervalMillis;

    private ServerConfig(int port, long expirationCleanupIntervalMillis) {
        this.port = port;
        this.expirationCleanupIntervalMillis = expirationCleanupIntervalMillis;
    }

    static ServerConfig fromArgs(String[] args) {
        int port = 6379;
        long cleanupIntervalMillis = 1000;

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
            }
        }

        return new ServerConfig(port, cleanupIntervalMillis);
    }
}
