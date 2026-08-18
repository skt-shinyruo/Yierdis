package yier.bubu.redis.storage.api;

import java.util.Objects;

public record DbEngineConfig(
        int dbIndex,
        long maxmemoryBytes,
        MaxmemoryPolicy maxmemoryPolicy,
        int maxmemorySamples,
        long evictionTimeLimitMillis,
        long expireCleanupTimeLimitMillis,
        DbDefragConfig defrag
) {
    public DbEngineConfig {
        if (dbIndex < 0) throw new IllegalArgumentException("dbIndex must be non-negative");
        if (maxmemoryBytes < 0L) throw new IllegalArgumentException("maxmemoryBytes must be non-negative");
        if (maxmemorySamples < 1) throw new IllegalArgumentException("maxmemorySamples must be positive");
        if (evictionTimeLimitMillis < 0L || expireCleanupTimeLimitMillis < 0L) {
            throw new IllegalArgumentException("maintenance limits must be non-negative");
        }
        maxmemoryPolicy = Objects.requireNonNull(maxmemoryPolicy, "maxmemoryPolicy");
        defrag = Objects.requireNonNull(defrag, "defrag");
    }
}
