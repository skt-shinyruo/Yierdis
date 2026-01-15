package yier.bubu.redis.db;

import java.util.Objects;

/**
 * Heap-backed TTL index implementation using {@link ByteArrayKeyspace}.
 * <p>
 * This preserves the existing "canonical key sharing" behavior where the expires index stores TTLs under the
 * store's canonical key instance.
 */
final class YierdisHeapExpireIndex implements YierdisExpireIndex {
    private final ByteArrayKeyspace<Long> expires = new ByteArrayKeyspace<>();

    @Override
    public int size() {
        return expires.size();
    }

    @Override
    public Long get(byte[] keyBytes) {
        return expires.get(keyBytes);
    }

    @Override
    public Long get(YierdisBytesView keyView) {
        return expires.get(keyView);
    }

    @Override
    public byte[] randomKey() {
        return expires.randomKey();
    }

    @Override
    public void clear() {
        expires.clear();
    }

    @Override
    public void setExpireAtMillis(byte[] keyBytes, long expireAtMillis, YierdisKeyspace<?> store) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Objects.requireNonNull(store, "store");

        byte[] canonical = store.canonicalKey(keyBytes);
        if (canonical == null) {
            canonical = keyBytes;
        }

        // If the expires key is stored under a different byte[] instance, migrate it to the canonical key.
        byte[] expiresKey = expires.canonicalKey(keyBytes);
        if (expiresKey != null && expiresKey != canonical) {
            Long current = expires.get(keyBytes);
            if (current != null) {
                expires.remove(keyBytes, current);
            }
        }

        expires.compute(canonical, (k, old) -> expireAtMillis);
    }

    @Override
    public void removeExpire(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        expires.computeIfPresent(keyBytes, (k, old) -> null);
    }

    ByteArrayKeyspace<Long> rawKeyspace() {
        return expires;
    }
}

