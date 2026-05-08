package yier.bubu.redis.execution.executor;

import java.util.Objects;

/**
 * Provides per-key state storage for {@link ExecutorTaskQueue}.\n
 * <p>
 * The provider must be safe under concurrent access.
 */
@FunctionalInterface
public interface ExecutorKeyStateProvider<K, T> {
    ExecutorKeyState<T> getOrCreate(K key);

    static <K, T> ExecutorKeyStateProvider<K, T> constant(ExecutorKeyState<T> state) {
        Objects.requireNonNull(state, "state");
        return ignored -> state;
    }
}

