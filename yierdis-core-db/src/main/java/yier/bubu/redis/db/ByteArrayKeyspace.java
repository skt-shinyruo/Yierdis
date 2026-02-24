package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.ops.ScanCursorV2;
import yier.bubu.redis.db.key.KeyHandle;

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
final class ByteArrayKeyspace<V> implements YierdisKeyspace<V> {
    // The following memory estimation constants intentionally trade precision for explainability and low cost.
    // They are used for diagnostics (MEMORY STATS / INFO), not for exact accounting.
    private static final int ESTIMATED_HEAP_ARRAY_HEADER_BYTES = 16;
    private static final int ESTIMATED_HEAP_REFERENCE_BYTES = 8;

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

    @Override
    public int size() {
        return size0 + size1;
    }

    boolean isRehashing() {
        return keys1 != null;
    }

    int table0Capacity() {
        return states0 == null ? 0 : states0.length;
    }

    int table1Capacity() {
        return states1 == null ? 0 : states1.length;
    }

    /**
     * Estimates the structural overhead of the hash table arrays (slot arrays) on heap.
     * <p>
     * Includes the backing arrays of the two tables during rehashing (Redis-style dual table period).
     * Does <b>not</b> include individual key {@code byte[]} object headers nor value payloads.
     */
    long estimatedTableOverheadBytes() {
        long bytes = estimateTableArrays(keys0, hashes0, values0, states0);
        if (keys1 != null) {
            bytes += estimateTableArrays(keys1, hashes1, values1, states1);
        }
        return bytes;
    }

    private static long estimateTableArrays(byte[][] keys, int[] hashes, Object[] values, byte[] states) {
        if (states == null) {
            return 0;
        }
        int cap = states.length;
        long bytes = 0;
        bytes += estimateRefArray(cap);   // keys
        bytes += estimateIntArray(cap);   // hashes
        bytes += estimateRefArray(cap);   // values
        bytes += estimateByteArray(cap);  // states
        return bytes;
    }

    private static long estimateRefArray(int len) {
        return estimatePrimitiveArrayBytes(len, ESTIMATED_HEAP_REFERENCE_BYTES);
    }

    private static long estimateIntArray(int len) {
        return estimatePrimitiveArrayBytes(len, Integer.BYTES);
    }

    private static long estimateByteArray(int len) {
        return estimatePrimitiveArrayBytes(len, 1);
    }

    private static long estimatePrimitiveArrayBytes(int len, int elementBytes) {
        if (len <= 0 || elementBytes <= 0) {
            return 0;
        }
        long data = (long) len * (long) elementBytes;
        long total = ESTIMATED_HEAP_ARRAY_HEADER_BYTES + data;
        return align8(total);
    }

    private static long align8(long bytes) {
        return (bytes + 7L) & ~7L;
    }

    @Override
    public V get(byte[] key) {
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

    @Override
    public V get(BytesView key) {
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

    @Override
    public V get(KeyHandle keyHandle) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        rehashStep();
        int h = keyHandle.dictHash();
        int idx = findIndex(keys0, hashes0, states0, keyHandle, h);
        if (idx >= 0) {
            @SuppressWarnings("unchecked")
            V v = (V) values0[idx];
            return v;
        }
        if (keys1 != null) {
            idx = findIndex(keys1, hashes1, states1, keyHandle, h);
            if (idx >= 0) {
                @SuppressWarnings("unchecked")
                V v = (V) values1[idx];
                return v;
            }
        }
        return null;
    }

    @Override
    public KeyHandle keyHandle(byte[] key) {
        Objects.requireNonNull(key, "key");
        rehashStep();
        int h = hash(key);
        int idx = findIndex(keys0, hashes0, states0, key, h);
        if (idx >= 0) {
            return KeyHandle.forHeap(keys0[idx], hashes0[idx]);
        }
        if (keys1 != null) {
            idx = findIndex(keys1, hashes1, states1, key, h);
            if (idx >= 0) {
                return KeyHandle.forHeap(keys1[idx], hashes1[idx]);
            }
        }
        return null;
    }

    @Override
    public KeyHandle keyHandle(BytesView key) {
        Objects.requireNonNull(key, "key");
        rehashStep();
        int h = hash(key);
        int idx = findIndex(keys0, hashes0, states0, key, h);
        if (idx >= 0) {
            return KeyHandle.forHeap(keys0[idx], hashes0[idx]);
        }
        if (keys1 != null) {
            idx = findIndex(keys1, hashes1, states1, key, h);
            if (idx >= 0) {
                return KeyHandle.forHeap(keys1[idx], hashes1[idx]);
            }
        }
        return null;
    }

    @Override
    public byte[] canonicalKey(byte[] key) {
        Objects.requireNonNull(key, "key");
        rehashStep();

        int h = hash(key);
        int idx = findIndex(keys0, hashes0, states0, key, h);
        if (idx >= 0) {
            return keys0[idx];
        }
        if (keys1 != null) {
            idx = findIndex(keys1, hashes1, states1, key, h);
            if (idx >= 0) {
                return keys1[idx];
            }
        }
        return null;
    }

    @Override
    public byte[] canonicalKey(BytesView key) {
        Objects.requireNonNull(key, "key");
        rehashStep();

        int h = hash(key);
        int idx = findIndex(keys0, hashes0, states0, key, h);
        if (idx >= 0) {
            return keys0[idx];
        }
        if (keys1 != null) {
            idx = findIndex(keys1, hashes1, states1, key, h);
            if (idx >= 0) {
                return keys1[idx];
            }
        }
        return null;
    }

    @Override
    public V compute(byte[] key, BiFunction<? super byte[], ? super V, ? extends V> remappingFunction) {
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

    @Override
    public V computeIfPresent(byte[] key, BiFunction<? super byte[], ? super V, ? extends V> remappingFunction) {
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

    @Override
    public V computeWithHandle(byte[] key, BiFunction<? super KeyHandle, ? super V, ? extends V> remappingFunction) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(remappingFunction, "remappingFunction");
        rehashStep();
        maybeStartRehashForInsert();

        int h = hash(key);
        Location loc = findLocation(key, h);
        V oldValue = loc == null ? null : loc.getValue();

        KeyHandle handle;
        if (loc == null) {
            handle = KeyHandle.forHeap(key, h);
        } else if (loc.table == 0) {
            handle = KeyHandle.forHeap(keys0[loc.index], hashes0[loc.index]);
        } else {
            handle = KeyHandle.forHeap(keys1[loc.index], hashes1[loc.index]);
        }

        V newValue = remappingFunction.apply(handle, oldValue);
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

    @Override
    public V computeIfPresentWithHandle(byte[] key, BiFunction<? super KeyHandle, ? super V, ? extends V> remappingFunction) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(remappingFunction, "remappingFunction");
        rehashStep();

        int h = hash(key);
        Location loc = findLocation(key, h);
        if (loc == null) {
            return null;
        }

        KeyHandle handle = loc.table == 0
                ? KeyHandle.forHeap(keys0[loc.index], hashes0[loc.index])
                : KeyHandle.forHeap(keys1[loc.index], hashes1[loc.index]);

        V newValue = remappingFunction.apply(handle, loc.getValue());
        if (newValue == null) {
            loc.remove();
            return null;
        }
        loc.setValue(newValue);
        return newValue;
    }

    @Override
    public boolean remove(byte[] key, V expectedValue) {
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

    @Override
    public boolean remove(KeyHandle keyHandle, V expectedValue) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        rehashStep();

        int h = keyHandle.dictHash();
        int idx0 = findIndex(keys0, hashes0, states0, keyHandle, h);
        if (idx0 >= 0) {
            @SuppressWarnings("unchecked")
            V v = (V) values0[idx0];
            if (v != expectedValue) {
                return false;
            }
            new Location(0, idx0).remove();
            return true;
        }

        if (keys1 != null) {
            int idx1 = findIndex(keys1, hashes1, states1, keyHandle, h);
            if (idx1 >= 0) {
                @SuppressWarnings("unchecked")
                V v = (V) values1[idx1];
                if (v != expectedValue) {
                    return false;
                }
                new Location(1, idx1).remove();
                return true;
            }
        }

        return false;
    }

    @Override
    public void clear() {
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

    @Override
    public void forEach(BiConsumer<byte[], V> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        // Intentionally does not call rehashStep(): iteration should not mutate structure.
        forEachTable(keys0, states0, values0, consumer);
        if (keys1 != null) {
            forEachTable(keys1, states1, values1, consumer);
        }
    }

    @Override
    public void forEachKeyHandle(BiConsumer<KeyHandle, V> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        // Intentionally does not call rehashStep(): iteration should not mutate structure.
        forEachKeyHandleTable(keys0, hashes0, states0, values0, consumer);
        if (keys1 != null) {
            forEachKeyHandleTable(keys1, hashes1, states1, values1, consumer);
        }
    }

    @Override
    public ScanCursorV2 scan(ScanCursorV2 cursor, int maxSteps, ScanConsumer<V> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        if (maxSteps <= 0) {
            throw new IllegalArgumentException("maxSteps must be > 0");
        }

        if (size() == 0) {
            return ScanCursorV2.start();
        }

        int phase = cursor == null ? 0 : cursor.phase();
        long posLong = cursor == null ? 0L : cursor.position();
        int pos = posLong <= 0 ? 0 : (posLong >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) posLong);

        int steps = 0;
        while (steps++ < maxSteps) {
            if (phase == 0) {
                if (pos >= states0.length) {
                    if (keys1 == null) {
                        return ScanCursorV2.start();
                    }
                    phase = 1;
                    pos = 0;
                    continue;
                }
                if (states0[pos] == STATE_FILLED) {
                    @SuppressWarnings("unchecked")
                    V v = (V) values0[pos];
                    KeyHandle handle = KeyHandle.forHeap(keys0[pos], hashes0[pos]);
                    pos++;
                    if (!consumer.accept(handle, v)) {
                        return ScanCursorV2.ofPhaseAndPosition(phase, pos);
                    }
                    continue;
                }
                pos++;
                continue;
            }

            if (phase == 1) {
                if (keys1 == null || states1 == null) {
                    return ScanCursorV2.start();
                }
                if (pos >= states1.length) {
                    return ScanCursorV2.start();
                }
                if (states1[pos] == STATE_FILLED) {
                    @SuppressWarnings("unchecked")
                    V v = (V) values1[pos];
                    KeyHandle handle = KeyHandle.forHeap(keys1[pos], hashes1[pos]);
                    pos++;
                    if (!consumer.accept(handle, v)) {
                        return ScanCursorV2.ofPhaseAndPosition(phase, pos);
                    }
                    continue;
                }
                pos++;
                continue;
            }

            // Unknown phase: best-effort reset/terminate.
            return ScanCursorV2.start();
        }

        return ScanCursorV2.ofPhaseAndPosition(phase, pos);
    }

    @Override
    public byte[] randomKey() {
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

    private void forEachKeyHandleTable(byte[][] keys, int[] hashes, byte[] states, Object[] values, BiConsumer<KeyHandle, V> consumer) {
        for (int i = 0; i < states.length; i++) {
            if (states[i] != STATE_FILLED) {
                continue;
            }
            @SuppressWarnings("unchecked")
            V v = (V) values[i];
            consumer.accept(KeyHandle.forHeap(keys[i], hashes[i]), v);
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

    private int hash(BytesView key) {
        int len = key.len();
        if (len < 0) {
            throw new IllegalArgumentException("key length must be non-negative");
        }
        int base = 1;
        for (int i = 0; i < len; i++) {
            base = 31 * base + key.byteAt(i);
        }

        int h = base ^ seed;
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

    private static int findIndex(byte[][] keys, int[] hashes, byte[] states, BytesView key, int hash) {
        int mask = keys.length - 1;
        int idx = hash & mask;
        while (true) {
            byte state = states[idx];
            if (state == STATE_EMPTY) {
                return -1;
            }
            if (state == STATE_FILLED && hashes[idx] == hash && equals(keys[idx], key)) {
                return idx;
            }
            idx = (idx + 1) & mask;
        }
    }

    private static boolean equals(byte[] storedKey, BytesView key) {
        int len = key.len();
        if (storedKey.length != len) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            if (storedKey[i] != key.byteAt(i)) {
                return false;
            }
        }
        return true;
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
