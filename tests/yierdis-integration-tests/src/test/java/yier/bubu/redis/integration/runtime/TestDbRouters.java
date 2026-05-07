package yier.bubu.redis.integration.runtime;

import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.execution.api.ServerSession;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.storage.api.DbEngine;

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
            public DbEngine dbFor(ServerSession session) {
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
