package yier.bubu.redis.execution.executor;

/**
 * I/O-side adapter for {@link ExecutorBackpressureController}.\n
 * <p>
 * Implementations are expected to bridge thread-affinity concerns (e.g. schedule Netty autoRead flips on
 * the channel event loop).
 */
public interface ExecutorBackpressureIo<K> {
    boolean isActive(K key);

    boolean isWritable(K key);

    void disableAutoRead(K key);

    void enableAutoRead(K key);

    /**
     * Registers a close callback for the key.\n
     * The callback should be invoked best-effort exactly once when the key becomes permanently inactive.
     */
    void onClose(K key, Runnable callback);
}

