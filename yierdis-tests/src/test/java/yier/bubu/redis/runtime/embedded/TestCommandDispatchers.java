package yier.bubu.redis.runtime.embedded;

import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.storage.api.DbEngine;

public final class TestCommandDispatchers {
    private TestCommandDispatchers() {
    }

    public static CommandDispatcher forInstance(YierdisInstance instance) {
        return EmbeddedCommandComposition.createDispatcher(instance);
    }

    public static CommandDispatcher forDb(DbEngine db, CommandModule... extraModules) {
        return EmbeddedCommandComposition.createDispatcher(db, extraModules);
    }
}
