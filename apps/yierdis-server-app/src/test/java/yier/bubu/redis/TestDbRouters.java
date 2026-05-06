package yier.bubu.redis;

import yier.bubu.redis.command.YierdisDbRouter;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.runtime.YierdisInstance;

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
            public DbEngine dbFor(yier.bubu.redis.contract.ServerSession session) {
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
