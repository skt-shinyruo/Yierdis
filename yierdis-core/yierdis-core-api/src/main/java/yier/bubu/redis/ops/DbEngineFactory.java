package yier.bubu.redis.ops;

/**
 * SPI for creating DB engines for an instance.
 * <p>
 * The runtime layer uses this abstraction to avoid hard-coding a concrete storage implementation.
 */
public interface DbEngineFactory {
    RuntimeDbEngine create(
            int dbIndex,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    );
}
