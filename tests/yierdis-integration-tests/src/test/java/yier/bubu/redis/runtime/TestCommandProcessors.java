package yier.bubu.redis.runtime;

import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.defaults.DefaultCommandModules;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.ops.DbEngine;

import java.util.ArrayList;
import java.util.List;

public final class TestCommandProcessors {
    private TestCommandProcessors() {
    }

    public static YierdisFastCommandProcessor forInstance(YierdisInstance instance) {
        return new YierdisFastCommandProcessor(DefaultCommandModules.create(TestDbRouters.forInstance(instance), null));
    }

    public static YierdisFastCommandProcessor forDb(DbEngine db, CommandModule... extraModules) {
        List<CommandModule> modules = new ArrayList<>();
        modules.add(DefaultCommandModules.create(db));
        if (extraModules != null) {
            for (CommandModule extraModule : extraModules) {
                modules.add(extraModule);
            }
        }
        return new YierdisFastCommandProcessor(modules.toArray(new CommandModule[0]));
    }
}
