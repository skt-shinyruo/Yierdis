package yier.bubu.redis.ops;

/**
 * Maxmemory coordination boundary.
 * <p>
 * Runtime can provide a global coordinator to engines so that maxmemory
 * enforcement is centralized and policy-consistent.
 */
public interface MaxmemoryCoordinator {
    /**
     * Prepare for a write that may grow the maxmemory-accounted dataset.
     * <p>
     * Callers must pass a best-effort estimate of additional bytes the write could add. The value must be
     * non-negative; {@code 0} means "no dataset growth" (e.g. in-place update).
     * <p>
     * Implementations may perform eviction and/or expired-key cleanup to make room. If the write cannot be
     * admitted under the configured policy, this method must throw a runtime exception (typically with
     * message {@link MaxmemoryErrors#OOM_ERR}).
     * <p>
     * Threading: callers may invoke this concurrently; implementations should be thread-safe.
     *
     * @param estimatedExtraBytes estimated extra bytes (>= 0)
     * @throws IllegalArgumentException if {@code estimatedExtraBytes < 0}
     * @throws RuntimeException         if the write is rejected (OOM / policy)
     */
    void prepareWrite(long estimatedExtraBytes);

    /**
     * Returns the next monotonically increasing LRU clock value for global LRU comparability.
     * <p>
     * Values returned from a single coordinator must be comparable across all participants attached to it
     * (i.e. the same global timeline). Implementations should be thread-safe and must not return decreasing
     * values.
     *
     * @return next clock value (monotonically increasing)
     */
    long nextLruClock();
}
