package yier.bubu.redis.db;

import yier.bubu.redis.offheap.api.OffHeapAllocator;
import yier.bubu.redis.ops.DbEngineFactory;
import yier.bubu.redis.ops.RuntimeDbEngine;

/**
 * Default {@link DbEngineFactory} that creates {@link YierdisDb} engines.
 */
public final class YierdisDbEngineFactory implements DbEngineFactory {
    @Override
    public RuntimeDbEngine create(
            int dbIndex,
            OffHeapAllocator offHeapAllocator,
            boolean ownsOffHeapAllocator,
            boolean offHeapKeysEnabled,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        return new YierdisDb(
                offHeapAllocator,
                ownsOffHeapAllocator,
                offHeapKeysEnabled,
                maxmemoryBytes,
                maxmemoryPolicy,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis
        );
    }
}
