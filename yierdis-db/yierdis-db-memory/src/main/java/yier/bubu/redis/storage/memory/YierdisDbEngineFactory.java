package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.DbEngineFactory;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.RuntimeDbEngine;

import java.util.Objects;

/**
 * Default {@link DbEngineFactory} that creates {@link YierdisDb} engines.
 */
public final class YierdisDbEngineFactory implements DbEngineFactory {
    private final YierdisFfmMemoryRuntime memoryRuntime;

    public YierdisDbEngineFactory() {
        this.memoryRuntime = null;
    }

    public YierdisDbEngineFactory(YierdisFfmMemoryRuntime memoryRuntime) {
        this.memoryRuntime = Objects.requireNonNull(memoryRuntime, "memoryRuntime");
    }

    @Override
    public RuntimeDbEngine create(
            int dbIndex,
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        MaxmemoryPolicy policy = Objects.requireNonNull(maxmemoryPolicy, "maxmemoryPolicy");
        if (memoryRuntime == null) {
            return YierdisDb.createWithOwnedFfmRuntime(
                    maxmemoryBytes,
                    policy,
                    maxmemorySamples,
                    evictionTimeLimitMillis,
                    expireCleanupTimeLimitMillis
            );
        }
        return YierdisDb.createWithSharedFfmRuntime(
                memoryRuntime,
                maxmemoryBytes,
                policy,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis
        );
    }
}
