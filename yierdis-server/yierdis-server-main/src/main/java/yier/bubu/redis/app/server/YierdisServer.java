package yier.bubu.redis.app.server;

import yier.bubu.redis.app.server.args.YierdisCliException;

public final class YierdisServer {
    private static final System.Logger LOG = System.getLogger(YierdisServer.class.getName());

    public static void main(String[] args) throws Exception {
        final ServerConfig config;
        try {
            config = ServerConfig.fromArgs(args);
        } catch (YierdisCliException e) {
            // fromArgs already printed error + usage; keep startup path clean.
            System.exit(e.exitCode());
            return; // keep compiler happy
        }
        if (config == null) {
            return;
        }
        try {
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
                            config.runtimeConfig().bind(), server.port());
                    server.awaitClose();
                } finally {
                    try {
                        Runtime.getRuntime().removeShutdownHook(shutdownHook);
                    } catch (IllegalStateException shuttingDown) {
                        // JVM shutdown is already in progress; the hook is executing independently.
                    }
                }
            }
        } catch (YierdisCliException e) {
            // 可预期配置错误：避免输出长堆栈，给出明确提示并使用稳定退出码。
            System.err.println(e.getMessage());
            System.exit(e.exitCode());
        }
    }

    private YierdisServer() {
    }
}
