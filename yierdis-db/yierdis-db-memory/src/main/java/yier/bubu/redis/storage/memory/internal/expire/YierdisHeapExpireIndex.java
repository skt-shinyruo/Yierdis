package yier.bubu.redis.storage.memory.internal.expire;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.keyspace.YierdisKeyspace;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Heap-backed TTL index retained until expire-index cleanup removes the legacy implementation.
 * <p>
 * This preserves the existing "canonical key sharing" behavior where the expires index stores TTLs under the
 * store's canonical key instance.
 */
public final class YierdisHeapExpireIndex implements YierdisExpireIndex {
    private final Map<ByteArrayKey, ExpireEntry> expires = new HashMap<>();

    @Override
    public int size() {
        return expires.size();
    }

    @Override
    public Long get(byte[] keyBytes) {
        ExpireEntry entry = expires.get(ByteArrayKey.of(keyBytes));
        return entry == null ? null : entry.expireAtMillis;
    }

    @Override
    public Long get(BytesView keyView) {
        ExpireEntry entry = expires.get(ByteArrayKey.of(keyView));
        return entry == null ? null : entry.expireAtMillis;
    }

    @Override
    public Long get(KeyHandle keyHandle) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        return get((BytesView) keyHandle);
    }

    @Override
    public byte[] randomKey() {
        if (expires.isEmpty()) {
            return null;
        }
        return expires.values().iterator().next().canonicalKey;
    }

    @Override
    public KeyHandle randomKeyHandle() {
        byte[] key = randomKey();
        if (key == null) {
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

        ByteArrayKey lookup = ByteArrayKey.of(keyBytes);
        ExpireEntry existing = expires.get(lookup);
        if (existing != null && existing.canonicalKey != canonical) {
            expires.remove(lookup);
        }

        expires.put(ByteArrayKey.of(canonical), new ExpireEntry(canonical, expireAtMillis));
    }

    @Override
    public void setExpireAtMillis(KeyHandle keyHandle, long expireAtMillis) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        byte[] canonical = keyBytes(keyHandle);
        expires.put(ByteArrayKey.of(canonical), new ExpireEntry(canonical, expireAtMillis));
    }

    @Override
    public void removeExpire(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        expires.remove(ByteArrayKey.of(keyBytes));
    }

    @Override
    public void removeExpire(KeyHandle keyHandle) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        expires.remove(ByteArrayKey.of((BytesView) keyHandle));
    }

    public boolean isRehashing() {
        return false;
    }

    public int table0Capacity() {
        return Math.max(16, nextPowerOfTwo((int) Math.ceil(expires.size() / 0.75d)));
    }

    public int table1Capacity() {
        return 0;
    }

    public long estimatedTableOverheadBytes() {
        return (long) table0Capacity() * (Long.BYTES + Integer.BYTES + 2L * Long.BYTES);
    }

    private static byte[] keyBytes(KeyHandle keyHandle) {
        byte[] out = new byte[keyHandle.len()];
        for (int i = 0; i < out.length; i++) {
            out[i] = keyHandle.byteAt(i);
        }
        return out;
    }

    private static int nextPowerOfTwo(int value) {
        int capacity = 1;
        while (capacity < value) {
            capacity <<= 1;
        }
        return capacity;
    }

    private record ExpireEntry(byte[] canonicalKey, long expireAtMillis) {
    }

    private static final class ByteArrayKey {
        private final byte[] bytes;
        private final int hash;

        private ByteArrayKey(byte[] bytes) {
            this.bytes = Objects.requireNonNull(bytes, "bytes");
            this.hash = Arrays.hashCode(bytes);
        }

        private static ByteArrayKey of(byte[] bytes) {
            return new ByteArrayKey(bytes);
        }

        private static ByteArrayKey of(BytesView view) {
            Objects.requireNonNull(view, "view");
            byte[] bytes = new byte[view.length()];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = view.getByte(i);
            }
            return new ByteArrayKey(bytes);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ByteArrayKey other && Arrays.equals(bytes, other.bytes);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
