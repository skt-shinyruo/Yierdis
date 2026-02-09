package yier.bubu.redis.db;

import yier.bubu.redis.ops.TtlOps;

import java.util.Objects;

final class YierdisDbTtlOps implements TtlOps {
    private final YierdisDb db;

    YierdisDbTtlOps(YierdisDb db) {
        this.db = Objects.requireNonNull(db, "db");
    }

    @Override
    public boolean expire(YierdisBytesView keyView, long seconds) {
        return db.expire(keyView, seconds);
    }

    @Override
    public boolean pexpire(YierdisBytesView keyView, long milliseconds) {
        return db.pexpire(keyView, milliseconds);
    }

    @Override
    public boolean expireAtSeconds(YierdisBytesView keyView, long unixSeconds) {
        return db.expireAtSeconds(keyView, unixSeconds);
    }

    @Override
    public boolean expireAtMillis(YierdisBytesView keyView, long unixMillis) {
        return db.expireAtMillis(keyView, unixMillis);
    }

    @Override
    public boolean persist(YierdisBytesView keyView) {
        return db.persist(keyView);
    }

    @Override
    public long ttlSeconds(YierdisBytesView keyView) {
        return db.ttlSeconds(keyView);
    }

    @Override
    public long ttlMillis(YierdisBytesView keyView) {
        return db.ttlMillis(keyView);
    }
}

