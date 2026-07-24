package yier.bubu.redis.storage.memory.internal.expire;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

// YierdisDbExpirationManager：将 YierdisDb 的过期维护入口收敛为 ExpirationManager 边界。

import yier.bubu.redis.storage.api.ExpirationManager;
import yier.bubu.redis.storage.memory.YierdisDbHealth;

import java.util.Objects;

public final class YierdisDbExpirationManager implements ExpirationManager {
    private final YierdisDbExpirationSupport expirationSupport;
    private final YierdisDbHealth health;

    public YierdisDbExpirationManager(YierdisDbExpirationSupport expirationSupport, YierdisDbHealth health) {
        this.expirationSupport = Objects.requireNonNull(expirationSupport, "expirationSupport");
        this.health = Objects.requireNonNull(health, "health");
    }

    @Override
    public void cleanupExpired() {
        health.requireWritable();
        expirationSupport.cleanupExpired();
    }
}
