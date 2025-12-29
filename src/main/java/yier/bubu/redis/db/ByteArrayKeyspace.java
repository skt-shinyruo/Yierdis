package yier.bubu.redis.db;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Top-level keyspace hash table (Redis-style) using open addressing and incremental rehashing.
 * <p>
 * Keys are {@code byte[]} compared by contents. Values are stored directly to avoid per-entry node objects.
 * This is intentionally minimal and <b>not</b> thread-safe.
 */
final class ByteArrayKeyspace<V> {
    private static final float LOAD_FACTOR = 0.75f;
    private static final int MIN_CAPACITY = 16;
    private static final byte STATE_EMPTY = 0;
    private static final byte STATE_FILLED = 1;
    private static final byte STATE_TOMBSTONE = 2;

    private final int seed;

    private byte[][] keys0;
    private int[] hashes0;
    private Object[] values0;
    private byte[] states0;
    private int size0;
    private int used0;
    private int threshold0;

    private byte[][] keys1;
    private int[] hashes1;
    private Object[] values1;
    private byte[] states1;
    private int size1;
    private int used1;
    private int threshold1;

    private int rehashIndex = -1;

    ByteArrayKeyspace() {
        this(MIN_CAPACITY);
    }

    ByteArrayKeyspace(int expectedSize) {
        this.seed = ThreadLocalRandom.current().nextInt();
        int cap = tableSizeFor(expectedSize, LOAD_FACTOR);
        initTable0(cap);
    }

    int size() {
        return size0 + size1;
    }

    boolean isRehashing() {
        return keys1 != null;
    }

    V get(byte[] key) {
        Objects.requireNonNull(key, "key");
        rehashStep();
        int h = hash(key);
        int idx = findIndex(keys0, hashes0, states0, key, h);
        if (idx >= 0) {
            @SuppressWarnings("unchecked")
            V v = (V) values0[idx];
            return v;
        }
        if (keys1 != null) {
            idx = findIndex(keys1, hashes1, states1, key, h);
            if (idx >= 0) {
                @SuppressWarnings("unchecked")
                V v = (V) values1[idx];
                return v;
            }
        }
        return null;
    }

    V compute(byte[] key, BiFunction<? super byte[], ? super V, ? extends V> remappingFunction) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(remappingFunction, "remappingFunction");
        rehashStep();
        maybeStartRehashForInsert();

        int h = hash(key);
        Location loc = findLocation(key, h);
        V oldValue = loc == null ? null : loc.getValue();
        V newValue = remappingFunction.apply(key, oldValue);

        if (newValue == null) {
            if (loc != null) {
                loc.remove();
            }
            return null;
        }

        if (loc != null) {
            loc.setValue(newValue);
            return newValue;
        }

        insertNew(key, h, newValue);
        return newValue;
    }

    V computeIfPresent(byte[] key, BiFunction<? super byte[], ? super V, ? extends V> remappingFunction) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(remappingFunction, "remappingFunction");
        rehashStep();

        int h = hash(key);
        Location loc = findLocation(key, h);
        if (loc == null) {
            return null;
        }
        V newValue = remappingFunction.apply(key, loc.getValue());
        if (newValue == null) {
            loc.remove();
            return null;
        }
        loc.setValue(newValue);
        return newValue;
    }

    boolean remove(byte[] key, V expectedValue) {
        Objects.requireNonNull(key, "key");
        rehashStep();

        int h = hash(key);
        Location loc = findLocation(key, h);
        if (loc == null) {
            return false;
        }
        if (loc.getValue() != expectedValue) {
            return false;
        }
        loc.remove();
        return true;
    }

    void clear() {
        initTable0(MIN_CAPACITY);
        keys1 = null;
        hashes1 = null;
        values1 = null;
        states1 = null;
        size1 = 0;
        used1 = 0;
        threshold1 = 0;
        rehashIndex = -1;
    }

    void forEach(BiConsumer<byte[], V> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        // Intentionally does not call rehashStep(): iteration should not mutate structure.
        forEachTable(keys0, states0, values0, consumer);
        if (keys1 != null) {
            forEachTable(keys1, states1, values1, consumer);
        }
    }

    byte[] randomKey() {
        rehashStep();
        int total = size();
        if (total == 0) {
            return null;
        }
        if (keys1 == null) {
            return randomKeyFromTable(keys0, states0);
        }

        int r = ThreadLocalRandom.current().nextInt(total);
        byte[] k = r < size0 ? randomKeyFromTable(keys0, states0) : randomKeyFromTable(keys1, states1);
        if (k != null) {
            return k;
        }
        // Fallback to the other table (can happen if one table is empty).
        return r < size0 ? randomKeyFromTable(keys1, states1) : randomKeyFromTable(keys0, states0);
    }

    private static byte[] randomKeyFromTable(byte[][] keys, byte[] states) {
        if (keys == null) {
            return null;
        }
        int len = states.length;
        if (len == 0) {
            return null;
        }
        int mask = len - 1;
        int start = ThreadLocalRandom.current().nextInt(len);

        int quickSteps = Math.min(16, len);
        for (int i = 0; i < quickSteps; i++) {
            int idx = (start + i) & mask;
            if (states[idx] == STATE_FILLED) {
                return keys[idx];
            }
        }

        for (int i = 0; i < len; i++) {
            int idx = (start + i) & mask;
            if (states[idx] == STATE_FILLED) {
                return keys[idx];
            }
        }
        return null;
    }

    private void forEachTable(byte[][] keys, byte[] states, Object[] values, BiConsumer<byte[], V> consumer) {
        for (int i = 0; i < states.length; i++) {
            if (states[i] != STATE_FILLED) {
                continue;
            }
            @SuppressWarnings("unchecked")
            V v = (V) values[i];
            consumer.accept(keys[i], v);
        }
    }

    private void initTable0(int capacity) {
        keys0 = new byte[capacity][];
        hashes0 = new int[capacity];
        values0 = new Object[capacity];
        states0 = new byte[capacity];
        size0 = 0;
        used0 = 0;
        threshold0 = (int) (capacity * LOAD_FACTOR);
    }

    private void initTable1(int capacity) {
        keys1 = new byte[capacity][];
        hashes1 = new int[capacity];
        values1 = new Object[capacity];
        states1 = new byte[capacity];
        size1 = 0;
        used1 = 0;
        threshold1 = (int) (capacity * LOAD_FACTOR);
    }

    private void maybeStartRehashForInsert() {
        if (keys1 != null) {
            return;
        }
        if (used0 + 1 <= threshold0) {
            return;
        }

        int target = tableSizeFor(size0 + 1, LOAD_FACTOR);
        if (target < MIN_CAPACITY) {
            target = MIN_CAPACITY;
        }
        startRehash(target);
    }

    private void maybeStartRehashForDeleteOrTombstones() {
        if (keys1 != null) {
            return;
        }

        int cap = keys0.length;
        if (size0 == 0) {
            if (cap > MIN_CAPACITY) {
                initTable0(MIN_CAPACITY);
            }
            return;
        }

        if (cap <= MIN_CAPACITY) {
            return;
        }

        int tombstones = used0 - size0;
        // Shrink when very sparse.
        if (size0 < cap / 8) {
            int target = tableSizeFor(size0, LOAD_FACTOR);
            if (target < MIN_CAPACITY) {
                target = MIN_CAPACITY;
            }
            if (target < cap) {
                startRehash(target);
                return;
            }
        }

        // Otherwise rebuild to clear tombstones (and possibly shrink if very sparse).
        if (tombstones > cap / 4) {
            int target = tableSizeFor(size0, LOAD_FACTOR);
            if (target < MIN_CAPACITY) {
                target = MIN_CAPACITY;
            }
            startRehash(target);
        }
    }

    private void startRehash(int capacity) {
        initTable1(capacity);
        rehashIndex = 0;
    }

    private void rehashStep() {
        if (keys1 == null) {
            return;
        }

        int steps = 1; // keep it small and predictable (Redis does small steps too).
        while (steps-- > 0 && keys1 != null) {
            while (rehashIndex < states0.length && states0[rehashIndex] != STATE_FILLED) {
                rehashIndex++;
            }
            if (rehashIndex >= states0.length) {
                finishRehash();
                return;
            }

            // Move this one slot from table0 -> table1.
            byte[] key = keys0[rehashIndex];
            int hash = hashes0[rehashIndex];
            @SuppressWarnings("unchecked")
            V value = (V) values0[rehashIndex];

            // Remove from old slot.
            states0[rehashIndex] = STATE_TOMBSTONE;
            keys0[rehashIndex] = null;
            values0[rehashIndex] = null;
            size0--;

            // Insert into new table.
            insertIntoTable1(key, hash, value);

            rehashIndex++;
        }
    }

    private void finishRehash() {
        keys0 = keys1;
        hashes0 = hashes1;
        values0 = values1;
        states0 = states1;
        size0 = size1;
        used0 = used1;
        threshold0 = threshold1;

        keys1 = null;
        hashes1 = null;
        values1 = null;
        states1 = null;
        size1 = 0;
        used1 = 0;
        threshold1 = 0;
        rehashIndex = -1;
    }

    private Location findLocation(byte[] key, int hash) {
        int idx0 = findIndex(keys0, hashes0, states0, key, hash);
        if (idx0 >= 0) {
            return new Location(0, idx0);
        }
        if (keys1 != null) {
            int idx1 = findIndex(keys1, hashes1, states1, key, hash);
            if (idx1 >= 0) {
                return new Location(1, idx1);
            }
        }
        return null;
    }

    private void insertNew(byte[] key, int hash, V value) {
        if (keys1 != null) {
            insertIntoTable1(key, hash, value);
            return;
        }
        insertIntoTable0(key, hash, value);
    }

    private void insertIntoTable0(byte[] key, int hash, V value) {
        int loc = findOrInsertLocation(keys0, hashes0, states0, key, hash);
        int insertAt = -loc - 1;
        if (states0[insertAt] == STATE_EMPTY) {
            used0++;
        }
        states0[insertAt] = STATE_FILLED;
        hashes0[insertAt] = hash;
        keys0[insertAt] = key;
        values0[insertAt] = value;
        size0++;
    }

    private void insertIntoTable1(byte[] key, int hash, V value) {
        if (used1 + 1 > threshold1) {
            // If the new table gets too full (should be rare with our sizing), grow it.
            growTable1();
        }
        int loc = findOrInsertLocation(keys1, hashes1, states1, key, hash);
        int insertAt = -loc - 1;
        if (states1[insertAt] == STATE_EMPTY) {
            used1++;
        }
        states1[insertAt] = STATE_FILLED;
        hashes1[insertAt] = hash;
        keys1[insertAt] = key;
        values1[insertAt] = value;
        size1++;
    }

    private void growTable1() {
        byte[][] oldKeys = keys1;
        int[] oldHashes = hashes1;
        Object[] oldValues = values1;
        byte[] oldStates = states1;
        int oldSize = size1;

        initTable1(oldKeys.length << 1);
        for (int i = 0; i < oldStates.length; i++) {
            if (oldStates[i] != STATE_FILLED) {
                continue;
            }
            byte[] k = oldKeys[i];
            int h = oldHashes[i];
            @SuppressWarnings("unchecked")
            V v = (V) oldValues[i];
            int loc = -findOrInsertLocation(keys1, hashes1, states1, k, h) - 1;
            states1[loc] = STATE_FILLED;
            hashes1[loc] = h;
            keys1[loc] = k;
            values1[loc] = v;
            size1++;
            used1++;
        }
        // Sanity: reinserted exactly oldSize elements.
        if (size1 != oldSize) {
            throw new IllegalStateException("rehash size mismatch");
        }
    }

    private int hash(byte[] key) {
        int h = java.util.Arrays.hashCode(key) ^ seed;
        h ^= (h >>> 16);
        h *= 0x7feb352d;
        h ^= (h >>> 15);
        h *= 0x846ca68b;
        h ^= (h >>> 16);
        return h == 0 ? 1 : h;
    }

    private static int findIndex(byte[][] keys, int[] hashes, byte[] states, byte[] key, int hash) {
        int mask = keys.length - 1;
        int idx = hash & mask;
        while (true) {
            byte state = states[idx];
            if (state == STATE_EMPTY) {
                return -1;
            }
            if (state == STATE_FILLED && hashes[idx] == hash && java.util.Arrays.equals(keys[idx], key)) {
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
    private static int findOrInsertLocation(byte[][] keys, int[] hashes, byte[] states, byte[] key, int hash) {
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
            } else if (hashes[idx] == hash && java.util.Arrays.equals(keys[idx], key)) {
                return idx;
            }
            idx = (idx + 1) & mask;
        }
    }

    private static int tableSizeFor(int expectedSize, float loadFactor) {
        int cap = 1;
        int needed = Math.max(4, (int) Math.ceil(expectedSize / loadFactor));
        while (cap < needed) {
            cap <<= 1;
        }
        return cap;
    }

    private final class Location {
        private final int table; // 0 or 1
        private final int index;

        private Location(int table, int index) {
            this.table = table;
            this.index = index;
        }

        V getValue() {
            @SuppressWarnings("unchecked")
            V v = (V) (table == 0 ? values0[index] : values1[index]);
            return v;
        }

        void setValue(V newValue) {
            if (table == 0) {
                values0[index] = newValue;
            } else {
                values1[index] = newValue;
            }
        }

        void remove() {
            if (table == 0) {
                states0[index] = STATE_TOMBSTONE;
                keys0[index] = null;
                values0[index] = null;
                size0--;
                maybeStartRehashForDeleteOrTombstones();
            } else {
                states1[index] = STATE_TOMBSTONE;
                keys1[index] = null;
                values1[index] = null;
                size1--;
            }
        }
    }
}

