package yier.bubu.redis.db;

import yier.bubu.redis.db.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.ops.DbEngineFactory;
import yier.bubu.redis.ops.MaxmemoryPolicy;
import yier.bubu.redis.ops.RuntimeDbEngine;

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
        String policyName = Objects.requireNonNull(maxmemoryPolicy, "maxmemoryPolicy").redisName();
        if (memoryRuntime == null) {
            return YierdisDb.createWithOwnedFfmRuntime(
                    maxmemoryBytes,
                    policyName,
                    maxmemorySamples,
                    evictionTimeLimitMillis,
                    expireCleanupTimeLimitMillis
            );
        }
        return YierdisDb.createWithSharedFfmRuntime(
                memoryRuntime,
                maxmemoryBytes,
                policyName,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis
        );
    }
}
