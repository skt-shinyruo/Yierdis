package yier.bubu.redis.db;

/**
 * Internal TTL index abstraction used by {@link YierdisDb}.
 * <p>
 * Implementations are intentionally minimal and are NOT thread-safe.
 */
public interface YierdisExpireIndex {
    int size();

    Long get(byte[] keyBytes);

    Long get(YierdisBytesView keyView);

    byte[] randomKey();

    void clear();

    void setExpireAtMillis(byte[] keyBytes, long expireAtMillis, YierdisKeyspace<?> store);

    void removeExpire(byte[] keyBytes);
}

