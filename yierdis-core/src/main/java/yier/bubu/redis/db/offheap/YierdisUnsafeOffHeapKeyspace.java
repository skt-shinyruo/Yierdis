package yier.bubu.redis.db.offheap;

import yier.bubu.redis.db.YierdisBytesView;
import yier.bubu.redis.db.YierdisKeyspace;
import yier.bubu.redis.db.ScanCursorV2;
import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.db.key.KeyHandleAccess;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAddressAllocator;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBlock;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * A Redis-style keyspace dictionary backed by Unsafe off-heap memory.
 * <p>
 * Keys are stored off-heap as raw byte blocks ({@code (addr,len)}), and the table slot arrays are stored off-heap.
 * Values are stored on-heap in a parallel {@code Object[]} to avoid storing Java references off-heap.
 */
public final class YierdisUnsafeOffHeapKeyspace<V> implements YierdisKeyspace<V> {
    private static final float LOAD_FACTOR = 0.75f;
    private static final int MIN_CAPACITY = 16;
    private static final byte STATE_EMPTY = 0;
    private static final byte STATE_FILLED = 1;
    private static final byte STATE_TOMBSTONE = 2;

    private static final int HASH_BYTES = Integer.BYTES;
    private static final int KEY_LEN_BYTES = Integer.BYTES;
    private static final int KEY_PTR_BYTES = Long.BYTES;
    private static final int ZERO_CHUNK_BYTES = 8 * 1024;
    private static final ThreadLocal<byte[]> TL_ZERO_CHUNK =
            ThreadLocal.withInitial(() -> new byte[ZERO_CHUNK_BYTES]);

    private final YierdisOffHeapAddressAllocator allocator;
    private final int seed;

    private Table table0;
    private Table table1;
    private int rehashIndex = -1;

    public YierdisUnsafeOffHeapKeyspace(YierdisOffHeapAddressAllocator allocator) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.seed = ThreadLocalRandom.current().nextInt();
    }

    @Override
    public int size() {
        Table t0 = table0;
        if (t0 == null) {
            return 0;
        }
        Table t1 = table1;
        return t0.size + (t1 == null ? 0 : t1.size);
    }

    @Override
    public V get(byte[] key) {
        Objects.requireNonNull(key, "key");
        Table t0 = table0;
        if (t0 == null) {
            return null;
        }
        rehashStep();
        int h = hash(key);
        int idx = findIndex(t0, key, h);
        if (idx >= 0) {
            @SuppressWarnings("unchecked")
            V v = (V) t0.values[idx];
            return v;
        }
        Table t1 = table1;
        if (t1 != null) {
            idx = findIndex(t1, key, h);
            if (idx >= 0) {
                @SuppressWarnings("unchecked")
                V v = (V) t1.values[idx];
                return v;
            }
        }
        return null;
    }

    @Override
    public V get(YierdisBytesView key) {
        Objects.requireNonNull(key, "key");
        Table t0 = table0;
        if (t0 == null) {
            return null;
        }
        rehashStep();
        int h = hash(key);
        int idx = findIndex(t0, key, h);
        if (idx >= 0) {
            @SuppressWarnings("unchecked")
            V v = (V) t0.values[idx];
            return v;
        }
        Table t1 = table1;
        if (t1 != null) {
            idx = findIndex(t1, key, h);
            if (idx >= 0) {
                @SuppressWarnings("unchecked")
                V v = (V) t1.values[idx];
                return v;
            }
        }
        return null;
    }

    @Override
    public V get(KeyHandle keyHandle) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        Table t0 = table0;
        if (t0 == null) {
            return null;
        }
        rehashStep();

        int h = keyHandle.dictHash();
        int keyLen = keyHandle.len();
        if (keyLen < 0) {
            throw new IllegalArgumentException("key length must be non-negative");
        }

        if (KeyHandleAccess.isOffHeap(keyHandle)) {
            if (KeyHandleAccess.offHeapAllocator(keyHandle) != allocator) {
                throw new IllegalArgumentException("key allocator mismatch: expected shared allocator");
            }
            long keyPtr = KeyHandleAccess.offHeapAddress(keyHandle);
            int idx = findIndexByPtr(t0, keyPtr, keyLen, h);
            if (idx >= 0) {
                @SuppressWarnings("unchecked")
                V v = (V) t0.values[idx];
                return v;
            }
            Table t1 = table1;
            if (t1 != null) {
                idx = findIndexByPtr(t1, keyPtr, keyLen, h);
                if (idx >= 0) {
                    @SuppressWarnings("unchecked")
                    V v = (V) t1.values[idx];
                    return v;
                }
            }
            return null;
        }

        int idx = findIndex(t0, keyHandle, h);
        if (idx >= 0) {
            @SuppressWarnings("unchecked")
            V v = (V) t0.values[idx];
            return v;
        }
        Table t1 = table1;
        if (t1 != null) {
            idx = findIndex(t1, keyHandle, h);
            if (idx >= 0) {
                @SuppressWarnings("unchecked")
                V v = (V) t1.values[idx];
                return v;
            }
        }
        return null;
    }

    @Override
    public byte[] canonicalKey(byte[] key) {
        Objects.requireNonNull(key, "key");
        Table t0 = table0;
        if (t0 == null) {
            return null;
        }
        rehashStep();
        int h = hash(key);
        int idx = findIndex(t0, key, h);
        if (idx >= 0) {
            return copyKeyBytes(t0, idx);
        }
        Table t1 = table1;
        if (t1 != null) {
            idx = findIndex(t1, key, h);
            if (idx >= 0) {
                return copyKeyBytes(t1, idx);
            }
        }
        return null;
    }

    @Override
    public byte[] canonicalKey(YierdisBytesView key) {
        Objects.requireNonNull(key, "key");
        Table t0 = table0;
        if (t0 == null) {
            return null;
        }
        rehashStep();
        int h = hash(key);
        int idx = findIndex(t0, key, h);
        if (idx >= 0) {
            return copyKeyBytes(t0, idx);
        }
        Table t1 = table1;
        if (t1 != null) {
            idx = findIndex(t1, key, h);
            if (idx >= 0) {
                return copyKeyBytes(t1, idx);
            }
        }
        return null;
    }

    @Override
    public V compute(byte[] key, BiFunction<? super byte[], ? super V, ? extends V> remappingFunction) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(remappingFunction, "remappingFunction");

        ensureTable0();
        rehashStep();
        maybeStartRehashForInsert();

        int h = hash(key);
        Location loc = findLocation(key, h);
        if (loc == null) {
            int keyLen = key.length;
            long keyPtr = 0L;
            if (keyLen > 0) {
                keyPtr = allocator.allocateAddress(keyLen);
                try {
                    allocator.copyMemory(key, 0, keyPtr, keyLen);
                } catch (RuntimeException e) {
                    allocator.freeAddress(keyPtr, keyLen);
                    throw e;
                }
            }

            V newValue;
            try {
                newValue = remappingFunction.apply(key, null);
            } catch (RuntimeException e) {
                if (keyLen > 0) {
                    allocator.freeAddress(keyPtr, keyLen);
                }
                throw e;
            }

            if (newValue == null) {
                if (keyLen > 0) {
                    allocator.freeAddress(keyPtr, keyLen);
                }
                return null;
            }
            insertNew(keyPtr, keyLen, h, newValue);
            return newValue;
        }

        V oldValue = loc.getValue();
        V newValue = remappingFunction.apply(key, oldValue);
        if (newValue == null) {
            loc.removeAndFreeKey();
            return null;
        }
        loc.setValue(newValue);
        return newValue;
    }

    @Override
    public V computeIfPresent(byte[] key, BiFunction<? super byte[], ? super V, ? extends V> remappingFunction) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(remappingFunction, "remappingFunction");

        Table t0 = table0;
        if (t0 == null) {
            return null;
        }

        rehashStep();
        int h = hash(key);
        Location loc = findLocation(key, h);
        if (loc == null) {
            return null;
        }
        V newValue = remappingFunction.apply(key, loc.getValue());
        if (newValue == null) {
            loc.removeAndFreeKey();
            return null;
        }
        loc.setValue(newValue);
        return newValue;
    }

    @Override
    public V computeWithHandle(byte[] key, BiFunction<? super KeyHandle, ? super V, ? extends V> remappingFunction) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(remappingFunction, "remappingFunction");

        ensureTable0();
        rehashStep();
        maybeStartRehashForInsert();

        int h = hash(key);
        Location loc = findLocation(key, h);
        if (loc == null) {
            int keyLen = key.length;
            long keyPtr = 0L;
            if (keyLen > 0) {
                keyPtr = allocator.allocateAddress(keyLen);
                try {
                    allocator.copyMemory(key, 0, keyPtr, keyLen);
                } catch (RuntimeException e) {
                    allocator.freeAddress(keyPtr, keyLen);
                    throw e;
                }
            }

            KeyHandle handle = KeyHandle.forOffHeap(allocator, keyPtr, keyLen, h);
            V newValue;
            try {
                newValue = remappingFunction.apply(handle, null);
            } catch (RuntimeException e) {
                if (keyLen > 0) {
                    allocator.freeAddress(keyPtr, keyLen);
                }
                throw e;
            }

            if (newValue == null) {
                if (keyLen > 0) {
                    allocator.freeAddress(keyPtr, keyLen);
                }
                return null;
            }

            insertNew(keyPtr, keyLen, h, newValue);
            return newValue;
        }

        V oldValue = loc.getValue();
        Table t = loc.table == 0 ? table0 : table1;
        long keyPtr = getLong(t.keyPtrAddr, loc.index);
        int keyLen = getInt(t.keyLenAddr, loc.index);
        int storedHash = getInt(t.hashesAddr, loc.index);
        KeyHandle handle = KeyHandle.forOffHeap(allocator, keyPtr, keyLen, storedHash);

        V newValue = remappingFunction.apply(handle, oldValue);
        if (newValue == null) {
            loc.removeAndFreeKey();
            return null;
        }
        loc.setValue(newValue);
        return newValue;
    }

    @Override
    public V computeIfPresentWithHandle(byte[] key, BiFunction<? super KeyHandle, ? super V, ? extends V> remappingFunction) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(remappingFunction, "remappingFunction");

        Table t0 = table0;
        if (t0 == null) {
            return null;
        }

        rehashStep();
        int h = hash(key);
        Location loc = findLocation(key, h);
        if (loc == null) {
            return null;
        }

        Table t = loc.table == 0 ? table0 : table1;
        long keyPtr = getLong(t.keyPtrAddr, loc.index);
        int keyLen = getInt(t.keyLenAddr, loc.index);
        int storedHash = getInt(t.hashesAddr, loc.index);
        KeyHandle handle = KeyHandle.forOffHeap(allocator, keyPtr, keyLen, storedHash);

        V newValue = remappingFunction.apply(handle, loc.getValue());
        if (newValue == null) {
            loc.removeAndFreeKey();
            return null;
        }
        loc.setValue(newValue);
        return newValue;
    }

    @Override
    public boolean remove(byte[] key, V expectedValue) {
        Objects.requireNonNull(key, "key");
        Table t0 = table0;
        if (t0 == null) {
            return false;
        }
        rehashStep();

        int h = hash(key);
        Location loc = findLocation(key, h);
        if (loc == null) {
            return false;
        }
        if (loc.getValue() != expectedValue) {
            return false;
        }
        loc.removeAndFreeKey();
        return true;
    }

    @Override
    public boolean remove(KeyHandle keyHandle, V expectedValue) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        Table t0 = table0;
        if (t0 == null) {
            return false;
        }
        rehashStep();

        int h = keyHandle.dictHash();
        int keyLen = keyHandle.len();
        if (keyLen < 0) {
            throw new IllegalArgumentException("key length must be non-negative");
        }

        Location loc = null;
        if (KeyHandleAccess.isOffHeap(keyHandle)) {
            if (KeyHandleAccess.offHeapAllocator(keyHandle) != allocator) {
                throw new IllegalArgumentException("key allocator mismatch: expected shared allocator");
            }
            long keyPtr = KeyHandleAccess.offHeapAddress(keyHandle);
            int idx0 = findIndexByPtr(t0, keyPtr, keyLen, h);
            if (idx0 >= 0) {
                loc = new Location(0, idx0);
            } else {
                Table t1 = table1;
                if (t1 != null) {
                    int idx1 = findIndexByPtr(t1, keyPtr, keyLen, h);
                    if (idx1 >= 0) {
                        loc = new Location(1, idx1);
                    }
                }
            }
        } else {
            int idx0 = findIndex(t0, keyHandle, h);
            if (idx0 >= 0) {
                loc = new Location(0, idx0);
            } else {
                Table t1 = table1;
                if (t1 != null) {
                    int idx1 = findIndex(t1, keyHandle, h);
                    if (idx1 >= 0) {
                        loc = new Location(1, idx1);
                    }
                }
            }
        }

        if (loc == null) {
            return false;
        }
        if (loc.getValue() != expectedValue) {
            return false;
        }
        loc.removeAndFreeKey();
        return true;
    }

    @Override
    public void clear() {
        Table t0 = table0;
        if (t0 != null) {
            freeAllKeysInTable(t0);
            t0.close();
        }
        Table t1 = table1;
        if (t1 != null) {
            freeAllKeysInTable(t1);
            t1.close();
        }
        table0 = null;
        table1 = null;
        rehashIndex = -1;
    }

    @Override
    public void forEach(BiConsumer<byte[], V> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        Table t0 = table0;
        if (t0 == null) {
            return;
        }
        forEachTable(t0, consumer);
        Table t1 = table1;
        if (t1 != null) {
            forEachTable(t1, consumer);
        }
    }

    @Override
    public void forEachKeyHandle(BiConsumer<KeyHandle, V> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        Table t0 = table0;
        if (t0 == null) {
            return;
        }
        forEachKeyHandleTable(t0, consumer);
        Table t1 = table1;
        if (t1 != null) {
            forEachKeyHandleTable(t1, consumer);
        }
    }

    @Override
    public ScanCursorV2 scan(ScanCursorV2 cursor, int maxSteps, ScanConsumer<V> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        if (maxSteps <= 0) {
            throw new IllegalArgumentException("maxSteps must be > 0");
        }

        Table t0 = table0;
        if (t0 == null || size() == 0) {
            return ScanCursorV2.start();
        }

        int phase = cursor == null ? 0 : cursor.phase();
        long posLong = cursor == null ? 0L : cursor.position();
        int pos = posLong <= 0 ? 0 : (posLong >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) posLong);

        int steps = 0;
        while (steps++ < maxSteps) {
            if (phase == 0) {
                if (pos >= t0.capacity) {
                    Table t1 = table1;
                    if (t1 == null) {
                        return ScanCursorV2.start();
                    }
                    phase = 1;
                    pos = 0;
                    continue;
                }
                if (getByte(t0.statesAddr, pos) == STATE_FILLED) {
                    @SuppressWarnings("unchecked")
                    V v = (V) t0.values[pos];
                    int h = getInt(t0.hashesAddr, pos);
                    long keyPtr = getLong(t0.keyPtrAddr, pos);
                    int keyLen = getInt(t0.keyLenAddr, pos);
                    if (keyLen < 0) {
                        keyLen = 0;
                    }
                    pos++;
                    if (!(keyPtr == 0 && keyLen > 0)) {
                        if (!consumer.accept(KeyHandle.forOffHeap(allocator, keyPtr, keyLen, h), v)) {
                            return ScanCursorV2.ofPhaseAndPosition(phase, pos);
                        }
                    }
                    continue;
                }
                pos++;
                continue;
            }

            if (phase == 1) {
                Table t1 = table1;
                if (t1 == null) {
                    return ScanCursorV2.start();
                }
                if (pos >= t1.capacity) {
                    return ScanCursorV2.start();
                }
                if (getByte(t1.statesAddr, pos) == STATE_FILLED) {
                    @SuppressWarnings("unchecked")
                    V v = (V) t1.values[pos];
                    int h = getInt(t1.hashesAddr, pos);
                    long keyPtr = getLong(t1.keyPtrAddr, pos);
                    int keyLen = getInt(t1.keyLenAddr, pos);
                    if (keyLen < 0) {
                        keyLen = 0;
                    }
                    pos++;
                    if (!(keyPtr == 0 && keyLen > 0)) {
                        if (!consumer.accept(KeyHandle.forOffHeap(allocator, keyPtr, keyLen, h), v)) {
                            return ScanCursorV2.ofPhaseAndPosition(phase, pos);
                        }
                    }
                    continue;
                }
                pos++;
                continue;
            }

            return ScanCursorV2.start();
        }

        return ScanCursorV2.ofPhaseAndPosition(phase, pos);
    }

    @Override
    public byte[] randomKey() {
        Table t0 = table0;
        if (t0 == null) {
            return null;
        }
        rehashStep();
        int total = size();
        if (total == 0) {
            return null;
        }

        Table t1 = table1;
        if (t1 == null) {
            return randomKeyFromTable(t0);
        }

        int r = ThreadLocalRandom.current().nextInt(total);
        byte[] k = r < t0.size ? randomKeyFromTable(t0) : randomKeyFromTable(t1);
        if (k != null) {
            return k;
        }
        return r < t0.size ? randomKeyFromTable(t1) : randomKeyFromTable(t0);
    }

    @Override
    public KeyHandle keyHandle(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Table t0 = table0;
        if (t0 == null) {
            return null;
        }
        rehashStep();
        int h = hash(keyBytes);
        int idx = findIndex(t0, keyBytes, h);
        if (idx >= 0) {
            return KeyHandle.forOffHeap(allocator, getLong(t0.keyPtrAddr, idx), getInt(t0.keyLenAddr, idx), h);
        }
        Table t1 = table1;
        if (t1 != null) {
            idx = findIndex(t1, keyBytes, h);
            if (idx >= 0) {
                return KeyHandle.forOffHeap(allocator, getLong(t1.keyPtrAddr, idx), getInt(t1.keyLenAddr, idx), h);
            }
        }
        return null;
    }

    @Override
    public KeyHandle keyHandle(YierdisBytesView keyView) {
        Objects.requireNonNull(keyView, "keyView");
        Table t0 = table0;
        if (t0 == null) {
            return null;
        }
        rehashStep();
        int h = hash(keyView);
        int idx = findIndex(t0, keyView, h);
        if (idx >= 0) {
            return KeyHandle.forOffHeap(allocator, getLong(t0.keyPtrAddr, idx), getInt(t0.keyLenAddr, idx), h);
        }
        Table t1 = table1;
        if (t1 != null) {
            idx = findIndex(t1, keyView, h);
            if (idx >= 0) {
                return KeyHandle.forOffHeap(allocator, getLong(t1.keyPtrAddr, idx), getInt(t1.keyLenAddr, idx), h);
            }
        }
        return null;
    }

    private void ensureTable0() {
        if (table0 != null) {
            return;
        }
        table0 = new Table(MIN_CAPACITY);
    }

    private void freeAllKeysInTable(Table t) {
        for (int i = 0; i < t.capacity; i++) {
            if (getByte(t.statesAddr, i) != STATE_FILLED) {
                continue;
            }
            long keyPtr = getLong(t.keyPtrAddr, i);
            int keyLen = getInt(t.keyLenAddr, i);
            if (keyPtr != 0 && keyLen > 0) {
                allocator.freeAddress(keyPtr, keyLen);
            }
            t.values[i] = null;
            putLong(t.keyPtrAddr, i, 0L);
            putInt(t.keyLenAddr, i, 0);
            putInt(t.hashesAddr, i, 0);
            putByte(t.statesAddr, i, STATE_EMPTY);
        }
        t.size = 0;
        t.used = 0;
    }

    private void forEachTable(Table t, BiConsumer<byte[], V> consumer) {
        for (int i = 0; i < t.capacity; i++) {
            if (getByte(t.statesAddr, i) != STATE_FILLED) {
                continue;
            }
            @SuppressWarnings("unchecked")
            V v = (V) t.values[i];
            consumer.accept(copyKeyBytes(t, i), v);
        }
    }

    private void forEachKeyHandleTable(Table t, BiConsumer<KeyHandle, V> consumer) {
        for (int i = 0; i < t.capacity; i++) {
            if (getByte(t.statesAddr, i) != STATE_FILLED) {
                continue;
            }
            @SuppressWarnings("unchecked")
            V v = (V) t.values[i];
            int h = getInt(t.hashesAddr, i);
            long keyPtr = getLong(t.keyPtrAddr, i);
            int keyLen = getInt(t.keyLenAddr, i);
            if (keyLen < 0) {
                keyLen = 0;
            }
            if (keyPtr == 0 && keyLen > 0) {
                continue;
            }
            consumer.accept(KeyHandle.forOffHeap(allocator, keyPtr, keyLen, h), v);
        }
    }

    private byte[] randomKeyFromTable(Table t) {
        int len = t.capacity;
        if (len == 0) {
            return null;
        }
        int mask = len - 1;
        int start = ThreadLocalRandom.current().nextInt(len);

        int quickSteps = Math.min(16, len);
        for (int i = 0; i < quickSteps; i++) {
            int idx = (start + i) & mask;
            if (getByte(t.statesAddr, idx) == STATE_FILLED) {
                return copyKeyBytes(t, idx);
            }
        }

        for (int i = 0; i < len; i++) {
            int idx = (start + i) & mask;
            if (getByte(t.statesAddr, idx) == STATE_FILLED) {
                return copyKeyBytes(t, idx);
            }
        }
        return null;
    }

    private byte[] copyKeyBytes(Table t, int index) {
        int keyLen = getInt(t.keyLenAddr, index);
        long keyPtr = getLong(t.keyPtrAddr, index);
        if (keyLen <= 0 || keyPtr == 0) {
            return new byte[0];
        }
        OffHeapKeyCopyDiagnostics.onHeapKeyCopy();
        byte[] out = new byte[keyLen];
        allocator.copyMemory(keyPtr, out, 0, keyLen);
        return out;
    }

    private void maybeStartRehashForInsert() {
        if (table1 != null) {
            return;
        }
        Table t0 = table0;
        if (t0.used + 1 <= t0.threshold) {
            return;
        }

        int target = tableSizeFor(t0.size + 1, LOAD_FACTOR);
        if (target < MIN_CAPACITY) {
            target = MIN_CAPACITY;
        }
        startRehash(target);
    }

    private void maybeStartRehashForDeleteOrTombstones() {
        if (table1 != null) {
            return;
        }
        Table t0 = table0;
        if (t0 == null) {
            return;
        }

        int cap = t0.capacity;
        if (t0.size == 0) {
            if (cap > MIN_CAPACITY) {
                t0.close();
                table0 = new Table(MIN_CAPACITY);
            }
            return;
        }

        if (cap <= MIN_CAPACITY) {
            return;
        }

        int tombstones = t0.used - t0.size;
        if (t0.size < cap / 8) {
            int target = tableSizeFor(t0.size, LOAD_FACTOR);
            if (target < MIN_CAPACITY) {
                target = MIN_CAPACITY;
            }
            if (target < cap) {
                startRehash(target);
                return;
            }
        }

        if (tombstones > cap / 4) {
            int target = tableSizeFor(t0.size, LOAD_FACTOR);
            if (target < MIN_CAPACITY) {
                target = MIN_CAPACITY;
            }
            startRehash(target);
        }
    }

    private void startRehash(int capacity) {
        table1 = new Table(capacity);
        rehashIndex = 0;
    }

    private void rehashStep() {
        if (table1 == null) {
            return;
        }

        int steps = 1;
        while (steps-- > 0 && table1 != null) {
            Table t0 = table0;
            Table t1 = table1;
            while (rehashIndex < t0.capacity && getByte(t0.statesAddr, rehashIndex) != STATE_FILLED) {
                rehashIndex++;
            }
            if (rehashIndex >= t0.capacity) {
                finishRehash();
                return;
            }

            int idx = rehashIndex;
            long keyPtr = getLong(t0.keyPtrAddr, idx);
            int keyLen = getInt(t0.keyLenAddr, idx);
            int hash = getInt(t0.hashesAddr, idx);
            @SuppressWarnings("unchecked")
            V value = (V) t0.values[idx];

            putByte(t0.statesAddr, idx, STATE_TOMBSTONE);
            putLong(t0.keyPtrAddr, idx, 0L);
            putInt(t0.keyLenAddr, idx, 0);
            putInt(t0.hashesAddr, idx, 0);
            t0.values[idx] = null;
            t0.size--;

            insertExistingIntoTable1(keyPtr, keyLen, hash, value);

            rehashIndex++;
        }
    }

    private void finishRehash() {
        Table old0 = table0;
        Table new0 = table1;

        old0.close();

        table0 = new0;
        table1 = null;
        rehashIndex = -1;
    }

    private Location findLocation(byte[] key, int hash) {
        Table t0 = table0;
        int idx0 = findIndex(t0, key, hash);
        if (idx0 >= 0) {
            return new Location(0, idx0);
        }
        Table t1 = table1;
        if (t1 != null) {
            int idx1 = findIndex(t1, key, hash);
            if (idx1 >= 0) {
                return new Location(1, idx1);
            }
        }
        return null;
    }

    private void insertNew(long keyPtr, int keyLen, int hash, V value) {
        Table t1 = table1;
        if (t1 != null) {
            if (t1.used + 1 > t1.threshold) {
                growTable1();
                t1 = table1;
            }
            insertIntoTable(t1, keyPtr, keyLen, hash, value);
            return;
        }
        insertIntoTable(table0, keyPtr, keyLen, hash, value);
    }

    private void insertExistingIntoTable1(long keyPtr, int keyLen, int hash, V value) {
        Table t1 = table1;
        if (t1.used + 1 > t1.threshold) {
            growTable1();
        }
        insertIntoTable(t1, keyPtr, keyLen, hash, value);
    }

    private void insertIntoTable(Table t, long keyPtr, int keyLen, int hash, V value) {
        int loc = findOrInsertLocation(t, keyPtr, keyLen, hash);
        int insertAt = -loc - 1;
        if (getByte(t.statesAddr, insertAt) == STATE_EMPTY) {
            t.used++;
        }
        putByte(t.statesAddr, insertAt, STATE_FILLED);
        putInt(t.hashesAddr, insertAt, hash);
        putLong(t.keyPtrAddr, insertAt, keyPtr);
        putInt(t.keyLenAddr, insertAt, keyLen);
        t.values[insertAt] = value;
        t.size++;
    }

    private void growTable1() {
        Table old = table1;
        int oldSize = old.size;
        Table next = new Table(old.capacity << 1);

        for (int i = 0; i < old.capacity; i++) {
            if (getByte(old.statesAddr, i) != STATE_FILLED) {
                continue;
            }
            long keyPtr = getLong(old.keyPtrAddr, i);
            int keyLen = getInt(old.keyLenAddr, i);
            int hash = getInt(old.hashesAddr, i);
            @SuppressWarnings("unchecked")
            V v = (V) old.values[i];

            int loc = -findOrInsertLocation(next, keyPtr, keyLen, hash) - 1;
            putByte(next.statesAddr, loc, STATE_FILLED);
            putInt(next.hashesAddr, loc, hash);
            putLong(next.keyPtrAddr, loc, keyPtr);
            putInt(next.keyLenAddr, loc, keyLen);
            next.values[loc] = v;
            next.size++;
            next.used++;
        }

        if (next.size != oldSize) {
            old.close();
            next.close();
            throw new IllegalStateException("rehash size mismatch");
        }

        old.close();
        table1 = next;
    }

    private int findIndex(Table t, byte[] key, int hash) {
        int mask = t.capacity - 1;
        int idx = hash & mask;
        int keyLen = key.length;
        while (true) {
            byte state = getByte(t.statesAddr, idx);
            if (state == STATE_EMPTY) {
                return -1;
            }
            if (state == STATE_FILLED && getInt(t.hashesAddr, idx) == hash) {
                int storedLen = getInt(t.keyLenAddr, idx);
                if (storedLen == keyLen && equalsKey(getLong(t.keyPtrAddr, idx), key, keyLen)) {
                    return idx;
                }
            }
            idx = (idx + 1) & mask;
        }
    }

    private int findIndex(Table t, YierdisBytesView key, int hash) {
        int mask = t.capacity - 1;
        int idx = hash & mask;
        int keyLen = key.len();
        while (true) {
            byte state = getByte(t.statesAddr, idx);
            if (state == STATE_EMPTY) {
                return -1;
            }
            if (state == STATE_FILLED && getInt(t.hashesAddr, idx) == hash) {
                int storedLen = getInt(t.keyLenAddr, idx);
                if (storedLen == keyLen && equalsKey(getLong(t.keyPtrAddr, idx), key, keyLen)) {
                    return idx;
                }
            }
            idx = (idx + 1) & mask;
        }
    }

    private int findIndexByPtr(Table t, long keyPtr, int keyLen, int hash) {
        int mask = t.capacity - 1;
        int idx = hash & mask;
        while (true) {
            byte state = getByte(t.statesAddr, idx);
            if (state == STATE_EMPTY) {
                return -1;
            }
            if (state == STATE_FILLED && getInt(t.hashesAddr, idx) == hash) {
                if (getLong(t.keyPtrAddr, idx) == keyPtr && getInt(t.keyLenAddr, idx) == keyLen) {
                    return idx;
                }
            }
            idx = (idx + 1) & mask;
        }
    }

    private int findOrInsertLocation(Table t, long keyPtr, int keyLen, int hash) {
        int mask = t.capacity - 1;
        int idx = hash & mask;
        int firstTombstone = -1;
        while (true) {
            byte state = getByte(t.statesAddr, idx);
            if (state == STATE_EMPTY) {
                int target = firstTombstone >= 0 ? firstTombstone : idx;
                return -(target + 1);
            }
            if (state == STATE_TOMBSTONE) {
                if (firstTombstone < 0) {
                    firstTombstone = idx;
                }
            } else if (getInt(t.hashesAddr, idx) == hash) {
                int storedLen = getInt(t.keyLenAddr, idx);
                long storedPtr = getLong(t.keyPtrAddr, idx);
                if (storedLen == keyLen && storedPtr == keyPtr) {
                    return idx;
                }
            }
            idx = (idx + 1) & mask;
        }
    }

    private boolean equalsKey(long storedPtr, byte[] key, int len) {
        for (int i = 0; i < len; i++) {
            if (allocator.getByte(storedPtr + i) != key[i]) {
                return false;
            }
        }
        return true;
    }

    private boolean equalsKey(long storedPtr, YierdisBytesView key, int len) {
        for (int i = 0; i < len; i++) {
            if (allocator.getByte(storedPtr + i) != key.byteAt(i)) {
                return false;
            }
        }
        return true;
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

    private int hash(YierdisBytesView key) {
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

    private static int tableSizeFor(int expectedSize, float loadFactor) {
        int cap = 1;
        int needed = Math.max(4, (int) Math.ceil(expectedSize / loadFactor));
        while (cap < needed) {
            cap <<= 1;
        }
        return cap;
    }

    private void clearMemory(long address, int bytes) {
        if (bytes <= 0) {
            return;
        }
        byte[] zeros = TL_ZERO_CHUNK.get();
        int remaining = bytes;
        long dst = address;
        while (remaining > 0) {
            int chunk = Math.min(remaining, zeros.length);
            allocator.copyMemory(zeros, 0, dst, chunk);
            dst += chunk;
            remaining -= chunk;
        }
    }

    private byte getByte(long base, int index) {
        return allocator.getByte(base + index);
    }

    private void putByte(long base, int index, byte value) {
        allocator.putByte(base + index, value);
    }

    private int getInt(long base, int index) {
        long addr = base + (long) index * HASH_BYTES;
        int b0 = allocator.getByte(addr) & 0xff;
        int b1 = allocator.getByte(addr + 1) & 0xff;
        int b2 = allocator.getByte(addr + 2) & 0xff;
        int b3 = allocator.getByte(addr + 3) & 0xff;
        return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
    }

    private void putInt(long base, int index, int value) {
        long addr = base + (long) index * HASH_BYTES;
        allocator.putByte(addr, (byte) value);
        allocator.putByte(addr + 1, (byte) (value >>> 8));
        allocator.putByte(addr + 2, (byte) (value >>> 16));
        allocator.putByte(addr + 3, (byte) (value >>> 24));
    }

    private long getLong(long base, int index) {
        long addr = base + (long) index * KEY_PTR_BYTES;
        long b0 = allocator.getByte(addr) & 0xffL;
        long b1 = allocator.getByte(addr + 1) & 0xffL;
        long b2 = allocator.getByte(addr + 2) & 0xffL;
        long b3 = allocator.getByte(addr + 3) & 0xffL;
        long b4 = allocator.getByte(addr + 4) & 0xffL;
        long b5 = allocator.getByte(addr + 5) & 0xffL;
        long b6 = allocator.getByte(addr + 6) & 0xffL;
        long b7 = allocator.getByte(addr + 7) & 0xffL;
        return b0
                | (b1 << 8)
                | (b2 << 16)
                | (b3 << 24)
                | (b4 << 32)
                | (b5 << 40)
                | (b6 << 48)
                | (b7 << 56);
    }

    private void putLong(long base, int index, long value) {
        long addr = base + (long) index * KEY_PTR_BYTES;
        allocator.putByte(addr, (byte) value);
        allocator.putByte(addr + 1, (byte) (value >>> 8));
        allocator.putByte(addr + 2, (byte) (value >>> 16));
        allocator.putByte(addr + 3, (byte) (value >>> 24));
        allocator.putByte(addr + 4, (byte) (value >>> 32));
        allocator.putByte(addr + 5, (byte) (value >>> 40));
        allocator.putByte(addr + 6, (byte) (value >>> 48));
        allocator.putByte(addr + 7, (byte) (value >>> 56));
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
            V v = (V) (table == 0 ? table0.values[index] : table1.values[index]);
            return v;
        }

        void setValue(V newValue) {
            if (table == 0) {
                table0.values[index] = newValue;
            } else {
                table1.values[index] = newValue;
            }
        }

        void removeAndFreeKey() {
            Table t = table == 0 ? table0 : table1;

            long keyPtr = getLong(t.keyPtrAddr, index);
            int keyLen = getInt(t.keyLenAddr, index);

            putByte(t.statesAddr, index, STATE_TOMBSTONE);
            putLong(t.keyPtrAddr, index, 0L);
            putInt(t.keyLenAddr, index, 0);
            putInt(t.hashesAddr, index, 0);
            t.values[index] = null;
            t.size--;

            if (keyPtr != 0 && keyLen > 0) {
                allocator.freeAddress(keyPtr, keyLen);
            }

            if (table == 0) {
                maybeStartRehashForDeleteOrTombstones();
            }
        }
    }

    private final class Table {
        final int capacity;
        final YierdisOffHeapBlock statesBlock;
        final YierdisOffHeapBlock hashesBlock;
        final YierdisOffHeapBlock keyPtrBlock;
        final YierdisOffHeapBlock keyLenBlock;

        final long statesAddr;
        final long hashesAddr;
        final long keyPtrAddr;
        final long keyLenAddr;
        final Object[] values;

        int size;
        int used;
        final int threshold;

        Table(int capacity) {
            this.capacity = capacity;
            this.statesBlock = allocator.allocateBlock(capacity);
            this.hashesBlock = allocator.allocateBlock(capacity * HASH_BYTES);
            this.keyPtrBlock = allocator.allocateBlock(capacity * KEY_PTR_BYTES);
            this.keyLenBlock = allocator.allocateBlock(capacity * KEY_LEN_BYTES);

            this.statesAddr = statesBlock.address();
            this.hashesAddr = hashesBlock.address();
            this.keyPtrAddr = keyPtrBlock.address();
            this.keyLenAddr = keyLenBlock.address();

            clearMemory(statesAddr, capacity);
            clearMemory(hashesAddr, capacity * HASH_BYTES);
            clearMemory(keyPtrAddr, capacity * KEY_PTR_BYTES);
            clearMemory(keyLenAddr, capacity * KEY_LEN_BYTES);

            this.values = new Object[capacity];
            this.threshold = (int) (capacity * LOAD_FACTOR);
        }

        void close() {
            statesBlock.close();
            hashesBlock.close();
            keyPtrBlock.close();
            keyLenBlock.close();
        }
    }
}
