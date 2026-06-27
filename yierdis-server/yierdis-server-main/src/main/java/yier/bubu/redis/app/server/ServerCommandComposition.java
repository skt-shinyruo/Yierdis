package yier.bubu.redis.app.server;

import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.SlowCommandGovernor;
import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.command.defaults.DefaultCommandModules;
import yier.bubu.redis.command.kernel.CommandRegistries;
import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.command.kernel.YierdisCommandProcessorOptions;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;

public final class ServerCommandComposition {
    private ServerCommandComposition() {
    }

    public static YierdisFastCommandProcessor createProcessor(
            YierdisCommandProcessorOptions options,
            YierdisDbRouter dbRouter,
            ServerInfoProvider infoProvider,
            SlowCommandGovernor slowGovernor
    ) {
        CommandRegistry registry = new CommandRegistry();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(options, registry);
        CommandRegistries.registerTransactionSupport(registry, processor::execute);
        CommandRegistries.registerInto(
                registry,
                DefaultCommandModules.create(dbRouter, infoProvider, slowGovernor),
                new ServerCommandModule(infoProvider)
        );
        return processor;
    }
}
