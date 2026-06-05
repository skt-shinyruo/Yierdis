package yier.bubu.redis.storage.memory.internal.expire;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandleAccess;

import java.util.Objects;

/**
 * Heap-backed TTL index implementation using {@link ByteArrayKeyspace}.
 * <p>
 * This preserves the existing "canonical key sharing" behavior where the expires index stores TTLs under the
 * store's canonical key instance.
 */
public final class YierdisHeapExpireIndex implements YierdisExpireIndex {
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
    public Long get(BytesView keyView) {
        return expires.get(keyView);
    }

    @Override
    public Long get(KeyHandle keyHandle) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        // Important: expires uses its own per-table seed, so we must not use keyHandle.dictHash() for indexing.
        return expires.get((BytesView) keyHandle);
    }

    @Override
    public byte[] randomKey() {
        return expires.randomKey();
    }

    @Override
    public KeyHandle randomKeyHandle() {
        byte[] k = expires.randomKey();
        if (k == null) {
            return null;
        }
        throw new UnsupportedOperationException("YierdisHeapExpireIndex no longer produces internal KeyHandle instances");
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
    public void setExpireAtMillis(KeyHandle keyHandle, long expireAtMillis) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        byte[] canonical = keyBytes(keyHandle);

        // Preserve canonical key sharing semantics: ensure the expires key is stored under the canonical key instance.
        byte[] expiresKey = expires.canonicalKey(canonical);
        if (expiresKey != null && expiresKey != canonical) {
            Long current = expires.get(canonical);
            if (current != null) {
                expires.remove(canonical, current);
            }
        }

        expires.compute(canonical, (k, old) -> expireAtMillis);
    }

    @Override
    public void removeExpire(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        expires.computeIfPresent(keyBytes, (k, old) -> null);
    }

    @Override
    public void removeExpire(KeyHandle keyHandle) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        byte[] canonical = keyBytes(keyHandle);
        expires.computeIfPresent(canonical, (k, old) -> null);
    }

    public ByteArrayKeyspace<Long> rawKeyspace() {
        return expires;
    }

    private static byte[] keyBytes(KeyHandle keyHandle) {
        byte[] out = new byte[keyHandle.len()];
        for (int i = 0; i < out.length; i++) {
            out[i] = keyHandle.byteAt(i);
        }
        return out;
    }
}
