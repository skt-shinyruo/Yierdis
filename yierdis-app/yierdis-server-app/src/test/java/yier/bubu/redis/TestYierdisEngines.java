package yier.bubu.redis;

import yier.bubu.redis.command.SlowCommandGovernor;
import yier.bubu.redis.engine.DefaultYierdisEngine;
import yier.bubu.redis.engine.YierdisEngine;
import yier.bubu.redis.runtime.YierdisInstance;

final class TestYierdisEngines {
    private TestYierdisEngines() {
    }

    static YierdisEngine forInstance(YierdisInstance instance) {
        return new DefaultYierdisEngine(
                TestDbRouters.forInstance(instance),
                null,
                SlowCommandGovernor.DEFAULT,
                () -> {
                }
        );
    }
}
