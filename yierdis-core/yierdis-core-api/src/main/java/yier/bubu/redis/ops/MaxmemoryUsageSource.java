package yier.bubu.redis.ops;

/**
 * A shared usage source that should be counted once for maxmemory.
 */
public interface MaxmemoryUsageSource {
    long usedBytes();
}

