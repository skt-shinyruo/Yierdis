package yier.bubu.redis.execution.executor;

/**
 * Runtime-state adapter for {@link ExecutorBackpressureController}.\n
 * <p>
 * This interface is intentionally minimal and designed for lock-free/atomic implementations.
 */
public interface ExecutorBackpressureRuntime<K> {
    int pending(K key);

    long pendingBytes(K key);

    boolean isClosing(K key);

    boolean markAutoReadDisabledByExecutor(K key);

    boolean autoReadDisabledByExecutor(K key);

    boolean clearAutoReadDisabledByExecutor(K key);

    default boolean inputPausedByReply(K key) {
        return false;
    }
}
