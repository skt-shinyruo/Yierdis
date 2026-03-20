package yier.bubu.redis.runtime;

import yier.bubu.redis.command.YierdisDbRouter;
import yier.bubu.redis.ops.DbEngine;

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
            public DbEngine dbFor(yier.bubu.redis.contract.DbIndexProvider dbIndexProvider) {
                if (engines.length == 0) {
                    throw new IllegalStateException("no dbs");
                }
                int idx = dbIndexProvider == null ? 0 : dbIndexProvider.dbIndex();
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
