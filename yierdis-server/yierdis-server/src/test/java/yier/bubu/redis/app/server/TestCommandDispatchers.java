package yier.bubu.redis.app.server;

import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;
import yier.bubu.redis.command.api.SlowCommandLimits;
import yier.bubu.redis.command.defaults.DefaultCommandModules;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.command.kernel.CommandRegistries;
import yier.bubu.redis.runtime.embedded.YierdisInstance;

final class TestCommandDispatchers {
    private TestCommandDispatchers() {
    }

    static CommandDispatcher forInstance(YierdisInstance instance) {
        NettyServerInfoProvider infoProvider = new NettyServerInfoProvider(
                runtimeConfig(0, 0, 1024, 1, 4, 5));
        return CommandRegistries.dispatcher(
                DefaultCommandModules.create(
                        YierdisServerBootstrap.dbRouter(instance),
                        infoProvider,
                        SlowCommandLimits.DEFAULT
                ),
                new ServerCommandModule(infoProvider)
        );
    }

    static YierdisServerRuntimeConfig runtimeConfig(
            int transactionQueueMaxCommands,
            long transactionQueueMaxBytes,
            int protocolMaxBulkBytes,
            int protocolMaxArgs,
            int protocolMaxInlineBytes,
            int protocolMaxCommandBytes
    ) {
        return ServerConfig.fromArgs(new String[]{
                "--maxmemoryBytes", "0",
                "--client-idle-timeout-millis", "300000",
                "--transactionQueueMaxCommands", Integer.toString(transactionQueueMaxCommands),
                "--transactionQueueMaxBytes", Long.toString(transactionQueueMaxBytes),
                "--protocolMaxBulkBytes", Integer.toString(protocolMaxBulkBytes),
                "--protocolMaxArgs", Integer.toString(protocolMaxArgs),
                "--protocolMaxLineBytes", Integer.toString(protocolMaxInlineBytes),
                "--protocolMaxCommandBytes", Integer.toString(protocolMaxCommandBytes)
        });
    }
}
