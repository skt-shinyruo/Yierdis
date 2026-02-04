package yier.bubu.redis.db;

import yier.bubu.redis.db.key.KeyHandle;

/**
 * Internal TTL index abstraction used by {@link YierdisDb}.
 * <p>
 * Implementations are intentionally minimal and are NOT thread-safe.
 */
public interface YierdisExpireIndex {
    int size();

    Long get(byte[] keyBytes);

    Long get(YierdisBytesView keyView);

    /**
     * Returns the TTL timestamp for a stable {@link KeyHandle} identity, or {@code null} if absent.
     * <p>
     * 约束：实现不得为查找而隐式生成 canonical heap key copy。
     */
    Long get(KeyHandle keyHandle);

    byte[] randomKey();

    /**
     * Returns a random stable key identity handle, or {@code null} if empty.
     * <p>
     * 约束：实现不得为返回 handle 而隐式生成 canonical heap key copy。
     */
    KeyHandle randomKeyHandle();

    void clear();

    void setExpireAtMillis(byte[] keyBytes, long expireAtMillis, YierdisKeyspace<?> store);

    /**
     * Sets TTL timestamp for a stable {@link KeyHandle} identity.
     * <p>
     * 约束：实现不得为 set 而隐式生成 canonical heap key copy。
     */
    void setExpireAtMillis(KeyHandle keyHandle, long expireAtMillis);

    void removeExpire(byte[] keyBytes);

    /**
     * Removes TTL entry for a stable {@link KeyHandle} identity.
     * <p>
     * 约束：实现不得为 remove 而隐式生成 canonical heap key copy。
     */
    void removeExpire(KeyHandle keyHandle);
}
