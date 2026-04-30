package yier.bubu.redis.db;

// YierdisDbExpirationManager：将 YierdisDb 的过期维护入口收敛为 ExpirationManager 边界。

import yier.bubu.redis.ops.ExpirationManager;

import java.util.Objects;

final class YierdisDbExpirationManager implements ExpirationManager {
    private final YierdisDbExpirationSupport expirationSupport;

    YierdisDbExpirationManager(YierdisDbExpirationSupport expirationSupport) {
        this.expirationSupport = Objects.requireNonNull(expirationSupport, "expirationSupport");
    }

    @Override
    public void cleanupExpired() {
        expirationSupport.cleanupExpired();
    }
}
