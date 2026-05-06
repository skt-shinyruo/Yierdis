package yier.bubu.redis.app.server;

import picocli.CommandLine;
import yier.bubu.redis.app.server.args.YierdisCliException;
import yier.bubu.redis.app.server.args.YierdisServerArgs;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;

import java.util.Objects;

final class ServerConfig {
    private final YierdisServerRuntimeConfig runtimeConfig;

    private ServerConfig(YierdisServerRuntimeConfig runtimeConfig) {
        this.runtimeConfig = Objects.requireNonNull(runtimeConfig, "runtimeConfig");
    }

    YierdisServerRuntimeConfig runtimeConfig() {
        return runtimeConfig;
    }

    static ServerConfig fromArgs(String[] args) {
        YierdisServerArgs parsed = new YierdisServerArgs();
        CommandLine cmd = new CommandLine(parsed);
        try {
            cmd.parseArgs(args);
        } catch (CommandLine.ParameterException e) {
            System.err.println(e.getMessage());
            cmd.usage(System.err);
            throw YierdisCliException.usageError(e.getMessage(), e);
        }

        if (parsed.help) {
            cmd.usage(System.out);
            return null;
        }

        try {
            parsed.normalizeAndValidate();
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            cmd.usage(System.err);
            throw YierdisCliException.usageError(e.getMessage(), e);
        }

        return new ServerConfig(parsed.toRuntimeConfig());
    }
}
