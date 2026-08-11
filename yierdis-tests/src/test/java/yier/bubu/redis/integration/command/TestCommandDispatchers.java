package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.SlowCommandGovernor;
import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.storage.api.DbEngine;

public final class TestCommandDispatchers {
    private TestCommandDispatchers() {
    }

    public static CommandDispatcher forDb(DbEngine db, CommandModule... extraModules) {
        return TestCommandComposition.createDispatcher(db, extraModules);
    }

    public static CommandDispatcher forDbWithSlowGovernor(DbEngine db, SlowCommandGovernor slowCommandGovernor) {
        return TestCommandComposition.createDispatcherWithSlowGovernor(db, slowCommandGovernor);
    }

    public static CommandDispatcher forRouter(
            YierdisDbRouter dbRouter,
            CommandModule... extraModules
    ) {
        return TestCommandComposition.createDispatcher(dbRouter, extraModules);
    }
}
