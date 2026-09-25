package yier.bubu.redis.app.server;

import lombok.extern.slf4j.Slf4j;

import yier.bubu.redis.app.server.args.YierdisCliException;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;

/**
 * 服务进程入口。
 * <p>
 * 职责只有三件事：把 argv 解析成运行时配置、以 try-with-resources 持有 {@link YierdisServerBootstrap}
 * 的生命周期、注册关闭钩子。真正的装配在 bootstrap 里，这里不碰 Netty 或 DB。
 * <p>
 * logger 就是普通的 {@code @Slf4j} static 字段。入口类的初始化发生在 {@code main} 之前，所以连参数错误这种
 * 根本用不到日志的路径，也会在 JVM 启动时初始化 Logback——建出日志目录和空的 {@code server.log}。
 * 这是刻意的取舍：不值得为了省下那一次文件创建，在入口类上多包一层延迟持有。
 */
@Slf4j
public final class YierdisServer {

    public static void main(String[] args) {
        final YierdisServerRuntimeConfig config;
        try {
            config = ServerConfig.fromArgs(args);
        } catch (YierdisCliException e) {
            System.exit(2);
            return;
        }

        try {
            run(config);
        } catch (Throwable runFailure) {
            // 走到这里说明服务没起来、或起来之后运行失败（start(...) 内部会关掉半初始化的实例）。
            // 这里补一条 ERROR 再以非 0 退出，避免失败只留下 JVM 原始的裸堆栈。
            log.error("yierdis run failed", runFailure);
            System.exit(1);
        }
    }

    private static void run(YierdisServerRuntimeConfig config) throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(config)) {
            Thread shutdownHook = new Thread(() -> {
                try {
                    server.close();
                } catch (Throwable failure) {
                    log.error("shutdown hook failed", failure);
                }
            }, "yierdis-shutdown-hook");
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            try {
                log.info("yierdis started on {}:{} (RESP)", config.bind(), server.port());
                server.awaitClose();
            } finally {
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } catch (IllegalStateException shuttingDown) {
                    // JVM 已在关闭流程中：钩子正在（或即将）执行 close()，这里不做别的。
                }
            }
        }
    }

    private YierdisServer() {
    }
}
