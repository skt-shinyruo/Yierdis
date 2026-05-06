package yier.bubu.redis.executor;

/**
 * Observability hook for {@link ExecutorBackpressureController}.\n
 * <p>
 * All callbacks are best-effort and should never throw.
 */
public interface ExecutorBackpressureObserver<K> {
    void onEnter(K key);

    void onExit(K key);
}

