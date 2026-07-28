package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.command.defaults.DefaultCommandModules;
import yier.bubu.redis.command.kernel.CommandRegistries;
import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.execution.api.DbIndexSession;
import yier.bubu.redis.storage.api.DbEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class TestCommandComposition {
    private TestCommandComposition() {
    }

    public static YierdisFastCommandProcessor createProcessor(DbEngine db, CommandModule... extraModules) {
        return createProcessor(singleDbRouter(db), extraModules);
    }

    public static YierdisFastCommandProcessor createProcessor(
            YierdisDbRouter dbRouter,
            CommandModule... extraModules
    ) {
        CommandRegistry registry = new CommandRegistry();
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(registry);
        CommandRegistries.registerTransactionSupport(registry, processor);
        List<CommandModule> modules = new ArrayList<>();
        modules.add(DefaultCommandModules.create(dbRouter, null));
        if (extraModules != null) {
            for (CommandModule extraModule : extraModules) {
                modules.add(extraModule);
            }
        }
        CommandRegistries.registerInto(registry, modules);
        registry.seal();
        return processor;
    }

    private static YierdisDbRouter singleDbRouter(DbEngine db) {
        DbEngine fixed = Objects.requireNonNull(db, "db");
        return new YierdisDbRouter() {
            @Override
            public DbEngine dbFor(DbIndexSession session) {
                return fixed;
            }

            @Override
            public int databases() {
                return 1;
            }
        };
    }

}
