package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.runtime.api.YierdisChangeSink;
import yier.bubu.redis.storage.api.DbEngine;

public final class TestCommandProcessors {
    private TestCommandProcessors() {
    }

    public static YierdisFastCommandProcessor forDb(DbEngine db, CommandModule... extraModules) {
        return TestCommandComposition.createProcessor(db, extraModules);
    }

    public static YierdisFastCommandProcessor forDbWithChangeSink(
            DbEngine db,
            YierdisChangeSink changeSink,
            CommandModule... extraModules
    ) {
        return TestCommandComposition.createProcessor(singleDbRouter(db), changeSink, extraModules);
    }

    public static YierdisFastCommandProcessor forRouter(
            YierdisDbRouter dbRouter,
            CommandModule... extraModules
    ) {
        return TestCommandComposition.createProcessor(dbRouter, YierdisChangeSink.NOOP, extraModules);
    }

    private static YierdisDbRouter singleDbRouter(DbEngine db) {
        return new YierdisDbRouter() {
            @Override
            public DbEngine dbFor(yier.bubu.redis.execution.api.DbIndexSession session) {
                return db;
            }

            @Override
            public int databases() {
                return 1;
            }
        };
    }
}
