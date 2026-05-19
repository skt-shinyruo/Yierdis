package yier.bubu.redis.app.server;

import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.runtime.embedded.YierdisInstance;

final class TestDbRouters {
    private TestDbRouters() {
    }

    static YierdisDbRouter forInstance(YierdisInstance instance) {
        if (instance == null) {
            throw new NullPointerException("instance");
        }
        DbEngine[] engines = instance.engines();
        return new YierdisDbRouter() {
            @Override
            public DbEngine dbFor(yier.bubu.redis.execution.api.DbIndexSession session) {
                if (engines.length == 0) {
                    throw new IllegalStateException("no dbs");
                }
                int idx = session == null ? 0 : session.dbIndex();
                if (idx < 0 || idx >= engines.length) {
                    idx = 0;
                }
                return engines[idx];
            }

            @Override
            public int databases() {
                return Math.max(1, engines.length);
            }
        };
    }
}
