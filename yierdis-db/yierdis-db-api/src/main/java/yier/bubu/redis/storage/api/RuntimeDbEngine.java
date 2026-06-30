package yier.bubu.redis.storage.api;

/**
 * Runtime engine instance contract.
 * <p>
 * This extends {@link DbEngine} with lifecycle hooks required by {@code yierdis-server-runtime}
 * assembly (thread binding + shutdown). The runtime uses this interface so that it does not
 * need to depend on concrete engine implementations (e.g. {@code YierdisDb}).
 */
public interface RuntimeDbEngine extends DbEngine, MaxmemoryParticipant, MaxmemoryCoordinatorAware {
    /**
     * Binds the engine to the current owner thread.
     */
    void bindToCurrentThread();

    /**
     * Runtime-only maintenance hook for background maxmemory enforcement.
     */
    void enforceMaxmemoryMaintenance();

    /**
     * Runtime-only maintenance hook for DB-local native allocator defrag.
     */
    default void defragMaintenance() {
        // Optional for engines without native allocator defrag.
    }

    /**
     * Best-effort shutdown / resource release.
     */
    void shutdown();

    @Override
    default void attachMaxmemoryCoordinator(MaxmemoryCoordinator coordinator) {
        // Optional for test doubles and lightweight runtimes.
    }

    /**
     * Returns an identity object for shared off-heap bytes that should be counted once across DBs,
     * or {@code null} when this engine does not participate in instance-level shared off-heap accounting.
     */
    default Object globalSharedOffHeapUsageIdentity() {
        return null;
    }

    /**
     * Returns the current shared off-heap bytes for the identity exposed by
     * {@link #globalSharedOffHeapUsageIdentity()}.
     */
    default long globalSharedOffHeapUsedBytes() {
        return 0L;
    }

    @Override
    default long usedBytesForMaxmemory() {
        return 0L;
    }

    @Override
    default int keyCountEstimate() {
        return 0;
    }

    @Override
    default void cleanupExpired(long nowMillis) {
        // Optional for test doubles and lightweight runtimes.
    }

    @Override
    default MaxmemoryCandidate sampleCandidate(MaxmemoryPolicy policy, long nowMillis) {
        return null;
    }

    @Override
    default boolean evict(MaxmemoryCandidate candidate, long nowMillis) {
        return false;
    }
}
