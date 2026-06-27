package yier.bubu.redis.runtime.embedded;

import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.defaults.DefaultCommandModules;
import yier.bubu.redis.command.kernel.CommandRegistries;
import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.storage.api.DbEngine;

import java.util.ArrayList;
import java.util.List;

public final class EmbeddedCommandComposition {
    private EmbeddedCommandComposition() {
    }

    public static YierdisFastCommandProcessor createProcessor(YierdisInstance instance) {
        CommandRegistry registry = new CommandRegistry();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        CommandRegistries.registerTransactionSupport(registry, processor::execute);
        CommandRegistries.registerInto(
                registry,
                DefaultCommandModules.create(TestDbRouters.forInstance(instance), null)
        );
        return processor;
    }

    public static YierdisFastCommandProcessor createProcessor(DbEngine db, CommandModule... extraModules) {
        CommandRegistry registry = new CommandRegistry();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        CommandRegistries.registerTransactionSupport(registry, processor::execute);
        List<CommandModule> modules = new ArrayList<>();
        modules.add(DefaultCommandModules.create(db));
        if (extraModules != null) {
            for (CommandModule extraModule : extraModules) {
                modules.add(extraModule);
            }
        }
        CommandRegistries.registerInto(registry, modules);
        return processor;
    }
}
