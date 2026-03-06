package yier.bubu.redis.ops;

import yier.bubu.redis.offheap.api.OffHeapAllocator;

/**
 * SPI for creating DB engines for an instance.
 * <p>
 * The runtime layer uses this abstraction to avoid hard-coding a concrete storage implementation.
 */
public interface DbEngineFactory {
    RuntimeDbEngine create(
            int dbIndex,
            OffHeapAllocator offHeapAllocator,
            boolean ownsOffHeapAllocator,
            boolean offHeapKeysEnabled,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    );
}
