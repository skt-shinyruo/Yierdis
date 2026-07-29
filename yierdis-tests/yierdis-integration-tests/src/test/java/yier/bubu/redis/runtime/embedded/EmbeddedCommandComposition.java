package yier.bubu.redis.runtime.embedded;

import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.defaults.DefaultCommandModules;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.command.kernel.CommandRegistries;
import yier.bubu.redis.storage.api.DbEngine;

import java.util.ArrayList;
import java.util.List;

public final class EmbeddedCommandComposition {
    private EmbeddedCommandComposition() {
    }

    public static CommandDispatcher createDispatcher(YierdisInstance instance) {
        return CommandRegistries.dispatcher(
                DefaultCommandModules.create(TestDbRouters.forInstance(instance), null)
        );
    }

    public static CommandDispatcher createDispatcher(DbEngine db, CommandModule... extraModules) {
        List<CommandModule> modules = new ArrayList<>();
        modules.add(DefaultCommandModules.create(db));
        if (extraModules != null) {
            for (CommandModule extraModule : extraModules) {
                modules.add(extraModule);
            }
        }
        return CommandRegistries.dispatcher(modules);
    }
}
