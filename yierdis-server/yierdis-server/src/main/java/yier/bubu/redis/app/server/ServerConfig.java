package yier.bubu.redis.app.server;

import yier.bubu.redis.app.server.args.YierdisCliException;
import yier.bubu.redis.app.server.args.YierdisServerFileConfig;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * 把启动配置解析成运行配置。服务不接受逐项启动参数，唯一的 argv 形态是 `--config <path>`
 * （默认 `./yierdis.conf`）；全部配置项的默认值与约束在 {@code docs/project-docs/configuration-and-operations.md}。
 * <p>
 * 失败路径只把原因打一行到 stderr（退出码由调用方给），**不打印 usage**：这个 jar 只有一个用途，就是启动服务，
 * 没有交互式用法可看。
 */
final class ServerConfig {
    private static final String CONFIG_OPTION = "--config";
    private static final Path DEFAULT_CONFIG = Path.of("yierdis.conf");

    private ServerConfig() {
    }

    static YierdisServerRuntimeConfig fromArgs(String[] args) {
        try {
            Path path = configPath(args);
            YierdisServerFileConfig config = YierdisServerFileConfig.fromProperties(loadProperties(path));
            if (!config.wasSpecified("maxmemoryBytes")) {
                throw new IllegalArgumentException(
                        "maxmemoryBytes must be specified explicitly in " + path
                                + " (use 0 to acknowledge unlimited memory)"
                );
            }
            config.normalizeAndValidate();
            return config.toRuntimeConfig();
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            throw YierdisCliException.invalidArguments(e.getMessage(), e);
        }
    }

    private static Path configPath(String[] args) {
        Path path = null;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            String inlineValue = null;
            if (arg.startsWith(CONFIG_OPTION + "=")) {
                inlineValue = arg.substring(CONFIG_OPTION.length() + 1);
            } else if (arg.equals(CONFIG_OPTION)) {
                if (++i >= args.length) {
                    throw new IllegalArgumentException("Missing required parameter for option '--config'");
                }
                inlineValue = args[i];
            } else {
                throw new IllegalArgumentException(
                        "Unknown argument: '" + arg + "' (only --config <path> is supported)"
                );
            }
            if (path != null) {
                throw new IllegalArgumentException("option '--config' may be specified only once");
            }
            path = Path.of(inlineValue);
        }
        if (path == null) {
            path = DEFAULT_CONFIG;
            if (!Files.isRegularFile(path)) {
                throw new IllegalArgumentException(
                        "no configuration file: expected ./yierdis.conf or pass --config <path>"
                );
            }
        } else if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("configuration file not found: " + path);
        }
        return path;
    }

    private static Properties loadProperties(Path path) {
        Properties props = new Properties();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            props.load(reader);
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to read configuration file '" + path + "': " + e.getMessage(), e);
        }
        return props;
    }
}
