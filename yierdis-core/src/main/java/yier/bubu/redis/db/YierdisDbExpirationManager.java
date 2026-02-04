package yier.bubu.redis.db;

// YierdisDbExpirationManager：将 YierdisDb 的过期维护入口收敛为 ExpirationManager 边界。

import yier.bubu.redis.ops.ExpirationManager;

import java.util.Objects;

final class YierdisDbExpirationManager implements ExpirationManager {
    private final YierdisDb db;

    YierdisDbExpirationManager(YierdisDb db) {
        this.db = Objects.requireNonNull(db, "db");
    }

    @Override
    public void cleanupExpired() {
        db.cleanupExpired();
    }
}

