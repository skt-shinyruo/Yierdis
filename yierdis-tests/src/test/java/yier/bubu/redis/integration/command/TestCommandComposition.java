package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.SlowCommandLimits;
import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.command.defaults.DefaultCommandModules;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.command.kernel.CommandRegistries;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.storage.api.DbEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class TestCommandComposition {
    private TestCommandComposition() {
    }

    public static CommandDispatcher createDispatcher(DbEngine db, CommandModule... extraModules) {
        return createDispatcher(singleDbRouter(db), extraModules);
    }

    public static CommandDispatcher createDispatcher(
            DbEngine db,
            ServerInfoProvider infoProvider
    ) {
        return CommandRegistries.dispatcher(DefaultCommandModules.create(
                singleDbRouter(db),
                infoProvider,
                SlowCommandLimits.DEFAULT
        ));
    }

    public static CommandDispatcher createDispatcherWithSlowLimits(
            DbEngine db,
            SlowCommandLimits slowCommandLimits
    ) {
        return CommandRegistries.dispatcher(DefaultCommandModules.create(
                singleDbRouter(db),
                null,
                slowCommandLimits
        ));
    }

    public static CommandDispatcher createDispatcher(
            YierdisDbRouter dbRouter,
            CommandModule... extraModules
    ) {
        List<CommandModule> modules = new ArrayList<>();
        modules.add(DefaultCommandModules.create(dbRouter, null, SlowCommandLimits.DEFAULT));
        if (extraModules != null) {
            for (CommandModule extraModule : extraModules) {
                modules.add(extraModule);
            }
        }
        return CommandRegistries.dispatcher(modules.toArray(CommandModule[]::new));
    }

    private static YierdisDbRouter singleDbRouter(DbEngine db) {
        DbEngine fixed = Objects.requireNonNull(db, "db");
        return new YierdisDbRouter() {
            @Override
            public DbEngine dbFor(CommandSession session) {
                return fixed;
            }

            @Override
            public int databases() {
                return 1;
            }
        };
    }

}
