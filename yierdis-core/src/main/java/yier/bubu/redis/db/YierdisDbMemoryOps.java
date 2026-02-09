package yier.bubu.redis.db;

import yier.bubu.redis.ops.MemoryOps;

import java.util.Objects;

final class YierdisDbMemoryOps implements MemoryOps {
    private final YierdisDb db;

    YierdisDbMemoryOps(YierdisDb db) {
        this.db = Objects.requireNonNull(db, "db");
    }

    @Override
    public long memoryUsage(YierdisBytesView keyView) {
        return db.memoryUsage(keyView);
    }

    @Override
    public YierdisMemoryStats memoryStats() {
        return db.memoryStats();
    }

    @Override
    public String objectEncoding(YierdisBytesView keyView) {
        return db.objectEncoding(keyView);
    }
}

