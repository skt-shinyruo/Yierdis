package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.api.MaxmemoryPolicy;

import java.util.concurrent.TimeUnit;

public final class YierdisDbConfig {
    final long maxmemoryBytes;
    final MaxmemoryPolicy maxmemoryPolicy;
    final int maxmemorySamples;
    final boolean lruEnabled;
    final long evictionTimeLimitNanos;
    final long expireCleanupTimeLimitNanos;

    private YierdisDbConfig(
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitNanos,
            long expireCleanupTimeLimitNanos
    ) {
        this.maxmemoryBytes = maxmemoryBytes;
        this.maxmemoryPolicy = maxmemoryPolicy;
        this.maxmemorySamples = maxmemorySamples;
        this.lruEnabled = maxmemoryBytes > 0 && maxmemoryPolicy == MaxmemoryPolicy.ALLKEYS_LRU;
        this.evictionTimeLimitNanos = evictionTimeLimitNanos;
        this.expireCleanupTimeLimitNanos = expireCleanupTimeLimitNanos;
    }

    static YierdisDbConfig create(
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        if (maxmemoryBytes < 0) {
            throw new IllegalArgumentException("maxmemoryBytes must be >= 0");
        }
        if (maxmemorySamples <= 0) {
            throw new IllegalArgumentException("maxmemorySamples must be > 0");
        }
        if (evictionTimeLimitMillis <= 0) {
            throw new IllegalArgumentException("evictionTimeLimitMillis must be > 0");
        }
        if (expireCleanupTimeLimitMillis <= 0) {
            throw new IllegalArgumentException("expireCleanupTimeLimitMillis must be > 0");
        }
        return new YierdisDbConfig(
                maxmemoryBytes,
                maxmemoryPolicy == null ? MaxmemoryPolicy.NOEVICTION : maxmemoryPolicy,
                maxmemorySamples,
                TimeUnit.MILLISECONDS.toNanos(evictionTimeLimitMillis),
                TimeUnit.MILLISECONDS.toNanos(expireCleanupTimeLimitMillis)
        );
    }
}
