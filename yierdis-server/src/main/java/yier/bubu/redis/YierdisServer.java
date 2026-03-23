package yier.bubu.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yier.bubu.redis.args.YierdisCliException;
import yier.bubu.redis.args.YierdisServerRuntimeConfig;

public final class YierdisServer {
    private static final Logger log = LoggerFactory.getLogger(YierdisServer.class);

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
        YierdisServerRuntimeConfig runtimeConfig = config.runtimeConfig();

        try {
            Integer exitCode = ForeignMemoryAutoModules.maybeRelaunchIfNeeded(runtimeConfig, args);
            if (exitCode != null) {
                System.exit(exitCode);
                return;
            }
            try (YierdisServerBootstrap server = YierdisServerBootstrap.start(config)) {
                log.info("yierdis started on 0.0.0.0:{} (Custom Protocol v1)", server.port());
                server.awaitClose();
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
