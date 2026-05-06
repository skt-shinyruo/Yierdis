package yier.bubu.redis.command;

import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.defaults.DefaultCommandModules;
import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.runtime.api.YierdisChangeSink;

import java.util.ArrayList;
import java.util.List;

public final class TestCommandProcessors {
    private TestCommandProcessors() {
    }

    public static YierdisFastCommandProcessor forDb(DbEngine db, CommandModule... extraModules) {
        return create(YierdisChangeSink.NOOP, DefaultCommandModules.create(db), extraModules);
    }

    public static YierdisFastCommandProcessor forDbWithChangeSink(
            DbEngine db,
            YierdisChangeSink changeSink,
            CommandModule... extraModules
    ) {
        return create(changeSink, DefaultCommandModules.create(db), extraModules);
    }

    public static YierdisFastCommandProcessor forRouter(
            YierdisDbRouter dbRouter,
            CommandModule... extraModules
    ) {
        return create(YierdisChangeSink.NOOP, DefaultCommandModules.create(dbRouter, null), extraModules);
    }

    private static YierdisFastCommandProcessor create(
            YierdisChangeSink changeSink,
            CommandModule defaults,
            CommandModule... extraModules
    ) {
        List<CommandModule> modules = new ArrayList<>();
        modules.add(defaults);
        if (extraModules != null) {
            for (CommandModule extraModule : extraModules) {
                modules.add(extraModule);
            }
        }
        return new YierdisFastCommandProcessor(changeSink, modules);
    }
}
