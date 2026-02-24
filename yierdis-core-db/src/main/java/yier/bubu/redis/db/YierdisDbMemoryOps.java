package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.ops.MemoryOps;
import yier.bubu.redis.ops.YierdisMemoryStats;

import java.util.Objects;

final class YierdisDbMemoryOps implements MemoryOps {
    private final YierdisDb db;

    YierdisDbMemoryOps(YierdisDb db) {
        this.db = Objects.requireNonNull(db, "db");
    }

    @Override
    public long memoryUsage(BytesView keyView) {
        return db.memoryUsage(keyView);
    }

    @Override
    public YierdisMemoryStats memoryStats() {
        return db.memoryStats();
    }

    @Override
    public String objectEncoding(BytesView keyView) {
        return db.objectEncoding(keyView);
    }
}
