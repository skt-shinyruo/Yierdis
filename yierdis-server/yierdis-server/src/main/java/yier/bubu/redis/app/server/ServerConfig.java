package yier.bubu.redis.app.server;

import yier.bubu.redis.app.server.args.YierdisCliException;
import yier.bubu.redis.app.server.args.YierdisServerArgs;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;

/**
 * 把 argv 解析成运行配置。
 * <p>
 * 失败路径只把原因打一行到 stderr（退出码由调用方给），**不打印 usage**：这个 jar 只有一个用途，就是启动服务，
 * 没有交互式用法可看；全部选项的默认值与约束在 {@code docs/project-docs/configuration-and-operations.md}。
 */
final class ServerConfig {
    private ServerConfig() {
    }

    static YierdisServerRuntimeConfig fromArgs(String[] args) {
        try {
            YierdisServerArgs parsed = YierdisServerArgs.parse(args);
            if (!parsed.wasSpecified("--maxmemoryBytes")) {
                throw new IllegalArgumentException(
                        "--maxmemoryBytes must be specified explicitly (use 0 to acknowledge unlimited memory)"
                );
            }
            parsed.normalizeAndValidate();
            return parsed.toRuntimeConfig();
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            throw YierdisCliException.invalidArguments(e.getMessage(), e);
        }
    }
}
