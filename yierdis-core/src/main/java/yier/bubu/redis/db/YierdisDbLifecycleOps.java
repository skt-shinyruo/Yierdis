package yier.bubu.redis.db;

import yier.bubu.redis.ops.DbLifecycleOps;

import java.util.Objects;

final class YierdisDbLifecycleOps implements DbLifecycleOps {
    private final YierdisDb db;

    YierdisDbLifecycleOps(YierdisDb db) {
        this.db = Objects.requireNonNull(db, "db");
    }

    @Override
    public void flushDb() {
        db.flushDb();
    }
}

