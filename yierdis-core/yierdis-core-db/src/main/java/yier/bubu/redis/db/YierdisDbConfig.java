package yier.bubu.redis.db;

import java.util.concurrent.TimeUnit;

final class YierdisDbConfig {
    final long maxmemoryBytes;
    final YierdisDb.MaxmemoryPolicy maxmemoryPolicy;
    final int maxmemorySamples;
    final boolean lruEnabled;
    final long evictionTimeLimitNanos;
    final long expireCleanupTimeLimitNanos;

    private YierdisDbConfig(
            long maxmemoryBytes,
            YierdisDb.MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitNanos,
            long expireCleanupTimeLimitNanos
    ) {
        this.maxmemoryBytes = maxmemoryBytes;
        this.maxmemoryPolicy = maxmemoryPolicy;
        this.maxmemorySamples = maxmemorySamples;
        this.lruEnabled = maxmemoryBytes > 0 && maxmemoryPolicy == YierdisDb.MaxmemoryPolicy.ALLKEYS_LRU;
        this.evictionTimeLimitNanos = evictionTimeLimitNanos;
        this.expireCleanupTimeLimitNanos = expireCleanupTimeLimitNanos;
    }

    static YierdisDbConfig create(
            long maxmemoryBytes,
            String maxmemoryPolicy,
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
                YierdisDbMaxmemoryPolicies.parseOrDefault(maxmemoryPolicy),
                maxmemorySamples,
                TimeUnit.MILLISECONDS.toNanos(evictionTimeLimitMillis),
                TimeUnit.MILLISECONDS.toNanos(expireCleanupTimeLimitMillis)
        );
    }
}
