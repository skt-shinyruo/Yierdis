package yier.bubu.redis.db;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;

/**
 * A tiny open-addressing hash map keyed by {@code byte[]}.
 * <p>
 * This is intentionally minimal and optimized for low object overhead.
 * It is <b>not</b> thread-safe.
 */
final class ByteArrayHashMap<V> {
    private static final float LOAD_FACTOR = 0.75f;
    private static final byte STATE_EMPTY = 0;
    private static final byte STATE_FILLED = 1;
    private static final byte STATE_TOMBSTONE = 2;

    private final int seed;

    private byte[][] keys;
    private int[] hashes;
    private Object[] values;
    private byte[] states;

    private int size = 0;
    private int used = 0;
    private int threshold;

    ByteArrayHashMap() {
        this(16);
    }

    ByteArrayHashMap(int expectedSize) {
        this.seed = ThreadLocalRandom.current().nextInt();
        int capacity = tableSizeFor(expectedSize, LOAD_FACTOR);
        initTables(capacity);
    }

    int size() {
        return size;
    }

    V get(byte[] key) {
        Objects.requireNonNull(key, "key");
        int idx = findIndex(key, hash(key));
        if (idx < 0) {
            return null;
        }
        @SuppressWarnings("unchecked")
        V v = (V) values[idx];
        return v;
    }

    boolean containsKey(byte[] key) {
        Objects.requireNonNull(key, "key");
        return findIndex(key, hash(key)) >= 0;
    }

    V put(byte[] key, V value) {
        Objects.requireNonNull(key, "key");
        ensureCapacityForInsert();
        int h = hash(key);
        int loc = findOrInsertLocation(key, h);
        if (loc >= 0) {
            @SuppressWarnings("unchecked")
            V old = (V) values[loc];
            values[loc] = value;
            return old;
        }

        int insertAt = -loc - 1;
        if (states[insertAt] == STATE_EMPTY) {
            used++;
        }
        states[insertAt] = STATE_FILLED;
        hashes[insertAt] = h;
        keys[insertAt] = key;
        values[insertAt] = value;
        size++;
        return null;
    }

    V remove(byte[] key) {
        Objects.requireNonNull(key, "key");
        int idx = findIndex(key, hash(key));
        if (idx < 0) {
            return null;
        }
        @SuppressWarnings("unchecked")
        V old = (V) values[idx];
        states[idx] = STATE_TOMBSTONE;
        keys[idx] = null;
        values[idx] = null;
        size--;
        return old;
    }

    boolean removeKey(byte[] key) {
        Objects.requireNonNull(key, "key");
        int idx = findIndex(key, hash(key));
        if (idx < 0) {
            return false;
        }
        states[idx] = STATE_TOMBSTONE;
        keys[idx] = null;
        values[idx] = null;
        size--;
        return true;
    }

    void forEach(BiConsumer<byte[], V> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        for (int i = 0; i < states.length; i++) {
            if (states[i] != STATE_FILLED) {
                continue;
            }
            @SuppressWarnings("unchecked")
            V v = (V) values[i];
            consumer.accept(keys[i], v);
        }
    }

    private void ensureCapacityForInsert() {
        if (used + 1 <= threshold) {
            return;
        }
        rehash(keys.length << 1);
    }

    private void rehash(int newCapacity) {
        byte[][] oldKeys = keys;
        int[] oldHashes = hashes;
        Object[] oldValues = values;
        byte[] oldStates = states;

        initTables(newCapacity);
        for (int i = 0; i < oldStates.length; i++) {
            if (oldStates[i] != STATE_FILLED) {
                continue;
            }
            byte[] k = oldKeys[i];
            int h = oldHashes[i];
            @SuppressWarnings("unchecked")
            V v = (V) oldValues[i];
            int loc = -findOrInsertLocation(k, h) - 1;
            states[loc] = STATE_FILLED;
            hashes[loc] = h;
            keys[loc] = k;
            values[loc] = v;
            size++;
            used++;
        }
    }

    private void initTables(int capacity) {
        keys = new byte[capacity][];
        hashes = new int[capacity];
        values = new Object[capacity];
        states = new byte[capacity];
        size = 0;
        used = 0;
        threshold = (int) (capacity * LOAD_FACTOR);
    }

    private int findIndex(byte[] key, int hash) {
        int mask = keys.length - 1;
        int idx = hash & mask;
        while (true) {
            byte state = states[idx];
            if (state == STATE_EMPTY) {
                return -1;
            }
            if (state == STATE_FILLED && hashes[idx] == hash && Arrays.equals(keys[idx], key)) {
                return idx;
            }
            idx = (idx + 1) & mask;
        }
    }

    /**
     * Returns:
     * <ul>
     *   <li>{@code >= 0}: existing key slot index</li>
     *   <li>{@code < 0}: insert slot index as {@code -(idx+1)}</li>
     * </ul>
     */
    private int findOrInsertLocation(byte[] key, int hash) {
        int mask = keys.length - 1;
        int idx = hash & mask;
        int firstTombstone = -1;
        while (true) {
            byte state = states[idx];
            if (state == STATE_EMPTY) {
                int target = firstTombstone >= 0 ? firstTombstone : idx;
                return -(target + 1);
            }
            if (state == STATE_TOMBSTONE) {
                if (firstTombstone < 0) {
                    firstTombstone = idx;
                }
            } else if (hashes[idx] == hash && Arrays.equals(keys[idx], key)) {
                return idx;
            }
            idx = (idx + 1) & mask;
        }
    }

    private int hash(byte[] key) {
        int h = Arrays.hashCode(key) ^ seed;
        // Murmur3-style finalization to smear patterns.
        h ^= (h >>> 16);
        h *= 0x7feb352d;
        h ^= (h >>> 15);
        h *= 0x846ca68b;
        h ^= (h >>> 16);
        // Ensure non-zero to avoid "hash == 0" becoming a weird sentinel.
        return h == 0 ? 1 : h;
    }

    private static int tableSizeFor(int expectedSize, float loadFactor) {
        int cap = 1;
        int needed = Math.max(4, (int) Math.ceil(expectedSize / loadFactor));
        while (cap < needed) {
            cap <<= 1;
        }
        return cap;
    }
}

