package yier.bubu.redis.runtime.embedded;

import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.storage.api.DbEngine;

public final class TestCommandProcessors {
    private TestCommandProcessors() {
    }

    public static YierdisFastCommandProcessor forInstance(YierdisInstance instance) {
        return EmbeddedCommandComposition.createProcessor(instance);
    }

    public static YierdisFastCommandProcessor forDb(DbEngine db, CommandModule... extraModules) {
        return EmbeddedCommandComposition.createProcessor(db, extraModules);
    }
}
