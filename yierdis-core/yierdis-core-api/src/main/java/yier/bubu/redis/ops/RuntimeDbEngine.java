package yier.bubu.redis.ops;

/**
 * Runtime engine instance contract.
 * <p>
 * This extends {@link DbEngine} with lifecycle hooks required by {@code yierdis-core-runtime}
 * assembly (thread binding + shutdown). The runtime uses this interface so that it does not
 * need to depend on concrete engine implementations (e.g. {@code YierdisDb}).
 */
public interface RuntimeDbEngine extends DbEngine {
    /**
     * Binds the engine to the current owner thread.
     */
    void bindToCurrentThread();

    /**
     * Runtime-only maintenance hook for background maxmemory enforcement.
     */
    void enforceMaxmemoryMaintenance();

    /**
     * Best-effort shutdown / resource release.
     */
    void shutdown();
}
