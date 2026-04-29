package yier.bubu.redis.runtime;

import yier.bubu.redis.command.DefaultCommandModules;
import yier.bubu.redis.command.YierdisFastCommandProcessor;

final class TestCommandProcessors {
    private TestCommandProcessors() {
    }

    static YierdisFastCommandProcessor forInstance(YierdisInstance instance) {
        return new YierdisFastCommandProcessor(DefaultCommandModules.create(TestDbRouters.forInstance(instance), null));
    }
}
