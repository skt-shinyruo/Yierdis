package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.ops.ScanCursorV2;
import yier.bubu.redis.db.key.KeyHandle;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Internal keyspace abstraction (Redis-style dictionary) used by {@link YierdisDb}.
 * <p>
 * Implementations are intentionally minimal and are NOT thread-safe.
 */
public interface YierdisKeyspace<V> {
    @FunctionalInterface
    interface ScanConsumer<V> {
        /**
         * @return {@code true} 继续扫描；{@code false} 停止本轮扫描并返回下一游标
         */
        boolean accept(KeyHandle keyHandle, V value);
    }

    int size();

    V get(byte[] key);

    V get(BytesView key);

    /**
     * Returns the stored value for a stable {@link KeyHandle} identity, or {@code null} if absent.
     * <p>
     * 约束：实现不得为查找而隐式生成 canonical heap key copy。
     */
    V get(KeyHandle keyHandle);

    /**
     * Returns a stored key identity handle for the given key, or {@code null} if absent.
     * <p>
     * 约束：实现不得为返回 handle 而隐式生成 canonical heap key copy；off-heap keys 模式下应返回基于地址的 handle。
     */
    KeyHandle keyHandle(byte[] key);

    /**
     * Returns a stored key identity handle for the given key view, or {@code null} if absent.
     * <p>
     * 约束：实现不得为返回 handle 而隐式生成 canonical heap key copy；off-heap keys 模式下应返回基于地址的 handle。
     */
    KeyHandle keyHandle(BytesView key);

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
    byte[] canonicalKey(BytesView key);

    V compute(byte[] key, BiFunction<? super byte[], ? super V, ? extends V> remappingFunction);

    V computeIfPresent(byte[] key, BiFunction<? super byte[], ? super V, ? extends V> remappingFunction);

    /**
     * Like {@link #compute(byte[], BiFunction)} but exposes a stable {@link KeyHandle} identity to the remapping function.
     * <p>
     * 约束：实现不得为生成 handle 而隐式生成 canonical heap key copy。
     */
    V computeWithHandle(byte[] key, BiFunction<? super KeyHandle, ? super V, ? extends V> remappingFunction);

    /**
     * Like {@link #computeIfPresent(byte[], BiFunction)} but exposes a stable {@link KeyHandle} identity to the remapping function.
     * <p>
     * 约束：实现不得为生成 handle 而隐式生成 canonical heap key copy。
     */
    V computeIfPresentWithHandle(byte[] key, BiFunction<? super KeyHandle, ? super V, ? extends V> remappingFunction);

    boolean remove(byte[] key, V expectedValue);

    /**
     * Removes the entry by stable {@link KeyHandle} identity.
     * <p>
     * 约束：实现不得为删除而隐式生成 canonical heap key copy。
     */
    boolean remove(KeyHandle keyHandle, V expectedValue);

    void clear();

    void forEach(BiConsumer<byte[], V> consumer);

    /**
     * Iterates all entries and exposes stable key identity via {@link KeyHandle}.
     * <p>
     * 该迭代接口用于避免 off-heap keys 模式下的 canonical heap copy（例如 SCAN/治理/统计等）。
     */
    void forEachKeyHandle(BiConsumer<KeyHandle, V> consumer);

    /**
     * Keyspace-level incremental scan (rehash-aware, best-effort).
     * <p>
     * 约束：
     * <ul>
     *   <li>实现不得隐式触发 canonical heap copy</li>
     *   <li>实现不得在 scan 过程中主动做 rehashStep（scan 应为只读；rehash 由写/查找路径推进）</li>
     * </ul>
     *
     * @param cursor   输入游标（{@code 0} 表示从头开始）
     * @param maxSteps 本轮扫描允许前进的最大 slot 步数（必须 &gt; 0，用于 time-slice）
     * @param consumer 每个 entry 的回调；返回 {@code false} 代表提前停止（例如达到 COUNT）
     * @return 下一游标；返回 {@code 0} 表示扫描结束
     */
    ScanCursorV2 scan(ScanCursorV2 cursor, int maxSteps, ScanConsumer<V> consumer);

    byte[] randomKey();

    /**
     * Returns a random stable key identity without materializing a canonical heap copy.
     */
    KeyHandle randomKeyHandle();
}
