package yier.bubu.redis.app.server;

import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.SlowCommandGovernor;
import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.command.defaults.DefaultCommandModules;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.command.kernel.CommandRegistries;

public final class ServerCommandComposition {
    private ServerCommandComposition() {
    }

    public static CommandDispatcher createDispatcher(
            YierdisDbRouter dbRouter,
            ServerInfoProvider infoProvider,
            SlowCommandGovernor slowGovernor
    ) {
        return CommandRegistries.dispatcher(
                DefaultCommandModules.create(dbRouter, infoProvider, slowGovernor),
                new ServerCommandModule(infoProvider)
        );
    }
}
