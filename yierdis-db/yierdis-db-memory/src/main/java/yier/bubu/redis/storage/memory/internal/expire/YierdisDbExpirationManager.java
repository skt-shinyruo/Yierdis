package yier.bubu.redis.storage.memory.internal.expire;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

// YierdisDbExpirationManager：将 YierdisDb 的过期维护入口收敛为 ExpirationManager 边界。

import yier.bubu.redis.storage.api.ExpirationManager;

import java.util.Objects;

public final class YierdisDbExpirationManager implements ExpirationManager {
    private final YierdisDbExpirationSupport expirationSupport;

    public YierdisDbExpirationManager(YierdisDbExpirationSupport expirationSupport) {
        this.expirationSupport = Objects.requireNonNull(expirationSupport, "expirationSupport");
    }

    @Override
    public void cleanupExpired() {
        expirationSupport.cleanupExpired();
    }
}
