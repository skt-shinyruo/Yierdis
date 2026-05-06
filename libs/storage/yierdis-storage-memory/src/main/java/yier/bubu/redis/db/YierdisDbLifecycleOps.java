package yier.bubu.redis.db;

import yier.bubu.redis.ops.DbLifecycleOps;
import yier.bubu.redis.ops.MutationOutcome;

import java.util.Objects;

final class YierdisDbLifecycleOps implements DbLifecycleOps {
    private final YierdisDb db;

    YierdisDbLifecycleOps(YierdisDb db) {
        this.db = Objects.requireNonNull(db, "db");
    }

    @Override
    public MutationOutcome flushDb() {
        return db.flushDb();
    }
}
