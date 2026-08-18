package yier.bubu.redis.app.server;

import yier.bubu.redis.app.server.args.YierdisCliException;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;

public final class YierdisServer {
    private static final System.Logger LOG = System.getLogger(YierdisServer.class.getName());

    public static void main(String[] args) throws Exception {
        final YierdisServerRuntimeConfig config;
        try {
            config = ServerConfig.fromArgs(args);
        } catch (YierdisCliException e) {
            System.exit(2);
            return;
        }
        if (config == null) {
            return;
        }
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(config)) {
            Thread shutdownHook = new Thread(() -> {
                try {
                    server.close();
                } catch (Throwable failure) {
                    LOG.log(System.Logger.Level.ERROR, "shutdown hook failed", failure);
                }
            }, "yierdis-shutdown-hook");
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            try {
                LOG.log(System.Logger.Level.INFO, "yierdis started on {0}:{1} (RESP)",
                        config.bind(), server.port());
                server.awaitClose();
            } finally {
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } catch (IllegalStateException shuttingDown) {
                    // JVM shutdown is already in progress; the hook is executing independently.
                }
            }
        }
    }

    private YierdisServer() {
    }
}
