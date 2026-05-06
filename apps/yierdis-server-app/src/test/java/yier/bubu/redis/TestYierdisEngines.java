package yier.bubu.redis;

import yier.bubu.redis.command.DefaultCommandModules;
import yier.bubu.redis.engine.DefaultYierdisEngine;
import yier.bubu.redis.engine.YierdisEngine;
import yier.bubu.redis.runtime.YierdisInstance;

final class TestYierdisEngines {
    private TestYierdisEngines() {
    }

    static YierdisEngine forInstance(YierdisInstance instance) {
        return new DefaultYierdisEngine(
                () -> {
                },
                DefaultCommandModules.create(TestDbRouters.forInstance(instance), null)
        );
    }
}
