package yier.bubu.redis.storage.api;

public record DbDefragConfig(
        boolean enabled,
        long maxMoveBytes,
        long maxObjects,
        long timeLimitMillis
) {
    public DbDefragConfig {
        if (maxMoveBytes < 0L || maxObjects < 0L || timeLimitMillis < 0L) {
            throw new IllegalArgumentException("defrag limits must be non-negative");
        }
    }
}
