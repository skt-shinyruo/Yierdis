package yier.bubu.redis.db;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Internal keyspace abstraction (Redis-style dictionary) used by {@link YierdisDb}.
 * <p>
 * Implementations are intentionally minimal and are NOT thread-safe.
 */
public interface YierdisKeyspace<V> {
    int size();

    V get(byte[] key);

    V get(YierdisBytesView key);

    /**
     * Returns a stored canonical key instance for the given key bytes, or {@code null} if absent.
     * <p>
     * For off-heap implementations this may return a heap copy.
     */
    byte[] canonicalKey(byte[] key);

    /**
     * Returns a stored canonical key instance for the given key bytes, or {@code null} if absent.
     * <p>
     * For off-heap implementations this may return a heap copy.
     */
    byte[] canonicalKey(YierdisBytesView key);

    V compute(byte[] key, BiFunction<? super byte[], ? super V, ? extends V> remappingFunction);

    V computeIfPresent(byte[] key, BiFunction<? super byte[], ? super V, ? extends V> remappingFunction);

    boolean remove(byte[] key, V expectedValue);

    void clear();

    void forEach(BiConsumer<byte[], V> consumer);

    byte[] randomKey();
}

