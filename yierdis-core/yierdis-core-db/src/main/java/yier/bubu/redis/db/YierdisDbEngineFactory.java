package yier.bubu.redis.db;

import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocator;
import yier.bubu.redis.ops.DbEngineFactory;
import yier.bubu.redis.ops.RuntimeDbEngine;

/**
 * Default {@link DbEngineFactory} that creates {@link YierdisDb} engines.
 */
public final class YierdisDbEngineFactory implements DbEngineFactory {
    @Override
    public RuntimeDbEngine create(
            int dbIndex,
            Object offHeapAllocator,
            boolean ownsOffHeapAllocator,
            boolean offHeapKeysEnabled,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        YierdisOffHeapAllocator allocator = null;
        if (offHeapAllocator instanceof YierdisOffHeapAllocator typed) {
            allocator = typed;
        } else if (offHeapAllocator != null) {
            throw new IllegalArgumentException("unsupported offHeapAllocator: " + offHeapAllocator.getClass().getName());
        }
        return new YierdisDb(
                allocator,
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
