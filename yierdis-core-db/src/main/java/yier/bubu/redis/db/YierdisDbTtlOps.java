package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.ops.TtlOps;

import java.util.Objects;

final class YierdisDbTtlOps implements TtlOps {
    private final YierdisDb db;

    YierdisDbTtlOps(YierdisDb db) {
        this.db = Objects.requireNonNull(db, "db");
    }

    @Override
    public boolean expire(BytesView keyView, long seconds) {
        return db.expire(keyView, seconds);
    }

    @Override
    public boolean pexpire(BytesView keyView, long milliseconds) {
        return db.pexpire(keyView, milliseconds);
    }

    @Override
    public boolean expireAtSeconds(BytesView keyView, long unixSeconds) {
        return db.expireAtSeconds(keyView, unixSeconds);
    }

    @Override
    public boolean expireAtMillis(BytesView keyView, long unixMillis) {
        return db.expireAtMillis(keyView, unixMillis);
    }

    @Override
    public boolean persist(BytesView keyView) {
        return db.persist(keyView);
    }

    @Override
    public long ttlSeconds(BytesView keyView) {
        return db.ttlSeconds(keyView);
    }

    @Override
    public long ttlMillis(BytesView keyView) {
        return db.ttlMillis(keyView);
    }
}

