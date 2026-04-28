package yier.bubu.redis.db.memory.ffm;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.db.YierdisKeyspace;
import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.db.key.KeyHandleAccess;
import yier.bubu.redis.db.memory.foreign.YierdisFfmAccess;
import yier.bubu.redis.db.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.db.memory.foreign.YierdisFfmRegion;
import yier.bubu.redis.db.memory.foreign.YierdisFfmSpan;
import yier.bubu.redis.ops.ScanCursorV2;

import java.util.Objects;
import java.util.ArrayDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public final class YierdisFfmKeyspace<V> implements YierdisKeyspace<V> {
    private static final float LOAD_FACTOR = 0.75f;
    private static final int MIN_CAPACITY = 16;
    private static final byte STATE_EMPTY = 0;
    private static final byte STATE_FILLED = 1;
    private static final byte STATE_TOMBSTONE = 2;

    private final YierdisFfmBlobStore blobStore;
    private final YierdisFfmMemoryRuntime memoryRuntime;
    private final ArrayDeque<Table> retiredTables = new ArrayDeque<>();

    private Table table0;
    private Table table1;
    private int rehashIndex = -1;

    public YierdisFfmKeyspace(YierdisFfmBlobStore blobStore) {
        this.blobStore = Objects.requireNonNull(blobStore, "blobStore");
        this.memoryRuntime = blobStore.memoryRuntime();
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

    public boolean isRehashing() {
        return table1 != null;
    }

    public int table0Capacity() {
        return table0 == null ? 0 : table0.capacity;
    }

    public int table1Capacity() {
        return table1 == null ? 0 : table1.capacity;
    }

    public long estimatedTableOverheadBytes() {
        return tableBytes(table0) + tableBytes(table1);
    }

    public long nativeBytes() {
        return blobStore.liveBytes() + estimatedTableOverheadBytes();
    }

    @Override
    public V get(byte[] key) {
        Objects.requireNonNull(key, "key");
        rehashStep();
        Table t0 = table0;
        if (t0 == null) {
            return null;
        }
        int h = hash(key);
        int idx = findIndex(t0, key, h);
        if (idx >= 0) {
            return t0.valueAt(idx);
        }
        Table t1 = table1;
        if (t1 != null) {
            idx = findIndex(t1, key, h);
            if (idx >= 0) {
                return t1.valueAt(idx);
            }
        }
        return null;
    }

    @Override
    public V get(BytesView key) {
        Objects.requireNonNull(key, "key");
        rehashStep();
        Table t0 = table0;
        if (t0 == null) {
            return null;
        }
        int h = hash(key);
        int idx = findIndex(t0, key, h);
        if (idx >= 0) {
            return t0.valueAt(idx);
        }
        Table t1 = table1;
        if (t1 != null) {
            idx = findIndex(t1, key, h);
            if (idx >= 0) {
                return t1.valueAt(idx);
            }
        }
        return null;
    }

    @Override
    public V get(KeyHandle keyHandle) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        rehashStep();
        Table t0 = table0;
        if (t0 == null) {
            return null;
        }
        int h = hash(keyHandle);
        int idx = findIndex(t0, keyHandle, h);
        if (idx >= 0) {
            return t0.valueAt(idx);
        }
        Table t1 = table1;
        if (t1 != null) {
            idx = findIndex(t1, keyHandle, h);
            if (idx >= 0) {
                return t1.valueAt(idx);
            }
        }
        return null;
    }

    @Override
    public KeyHandle keyHandle(byte[] key) {
        Objects.requireNonNull(key, "key");
        rehashStep();
        Table t0 = table0;
        if (t0 == null) {
            return null;
        }
        int h = hash(key);
        int idx = findIndex(t0, key, h);
        if (idx >= 0) {
            return KeyHandle.forFfm(t0.refs[idx], t0.hashAt(idx));
        }
        Table t1 = table1;
        if (t1 != null) {
            idx = findIndex(t1, key, h);
            if (idx >= 0) {
                return KeyHandle.forFfm(t1.refs[idx], t1.hashAt(idx));
            }
        }
        return null;
    }

    @Override
    public KeyHandle keyHandle(BytesView key) {
        Objects.requireNonNull(key, "key");
        rehashStep();
        Table t0 = table0;
        if (t0 == null) {
            return null;
        }
        int h = hash(key);
        int idx = findIndex(t0, key, h);
        if (idx >= 0) {
            return KeyHandle.forFfm(t0.refs[idx], t0.hashAt(idx));
        }
        Table t1 = table1;
        if (t1 != null) {
            idx = findIndex(t1, key, h);
            if (idx >= 0) {
                return KeyHandle.forFfm(t1.refs[idx], t1.hashAt(idx));
            }
        }
        return null;
    }

    @Override
    public byte[] canonicalKey(byte[] key) {
        Objects.requireNonNull(key, "key");
        KeyHandle handle = keyHandle(key);
        return handle == null ? null : blobStore.toByteArray(KeyHandleAccess.ffmBytesRef(handle));
    }

    @Override
    public byte[] canonicalKey(BytesView key) {
        Objects.requireNonNull(key, "key");
        KeyHandle handle = keyHandle(key);
        return handle == null ? null : blobStore.toByteArray(KeyHandleAccess.ffmBytesRef(handle));
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
        insertNew(blobStore.store(key), h, newValue);
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
        ensureTable0();
        rehashStep();
        maybeStartRehashForInsert();

        int h = hash(key);
        Location loc = findLocation(key, h);
        if (loc == null) {
            YierdisFfmBytesRef ref = blobStore.store(key);
            KeyHandle handle = KeyHandle.forFfm(ref, h);
            V newValue;
            try {
                newValue = remappingFunction.apply(handle, null);
            } catch (RuntimeException e) {
                blobStore.release(ref);
                throw e;
            }
            if (newValue == null) {
                blobStore.release(ref);
                return null;
            }
            insertNew(ref, h, newValue);
            return newValue;
        }

        V newValue = remappingFunction.apply(loc.keyHandle(), loc.getValue());
        if (newValue == null) {
            loc.remove();
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
        V newValue = remappingFunction.apply(loc.keyHandle(), loc.getValue());
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
        Table t0 = table0;
        if (t0 == null) {
            return false;
        }
        rehashStep();
        int h = hash(key);
        Location loc = findLocation(key, h);
        if (loc == null || loc.getValue() != expectedValue) {
            return false;
        }
        loc.remove();
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
        int h = hash(keyHandle);
        Location loc = findLocation(keyHandle, h);
        if (loc == null || loc.getValue() != expectedValue) {
            return false;
        }
        loc.remove();
        return true;
    }

    @Override
    public void clear() {
        closeRetiredTables();
        clearTable(table0);
        clearTable(table1);
        table0 = null;
        table1 = null;
        rehashIndex = -1;
    }

    @Override
    public void forEach(BiConsumer<byte[], V> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        forEachTable(table0, consumer);
        forEachTable(table1, consumer);
    }

    @Override
    public void forEachKeyHandle(BiConsumer<KeyHandle, V> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        forEachKeyHandleTable(table0, consumer);
        forEachKeyHandleTable(table1, consumer);
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
                Table t0 = table0;
                if (t0 == null) {
                    return ScanCursorV2.start();
                }
                if (pos >= t0.capacity) {
                    if (table1 == null) {
                        return ScanCursorV2.start();
                    }
                    phase = 1;
                    pos = 0;
                    continue;
                }
                if (t0.stateAt(pos) == STATE_FILLED) {
                    V v = t0.valueAt(pos);
                    KeyHandle handle = KeyHandle.forFfm(t0.refs[pos], t0.hashAt(pos));
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
                Table t1 = table1;
                if (t1 == null) {
                    return ScanCursorV2.start();
                }
                if (pos >= t1.capacity) {
                    return ScanCursorV2.start();
                }
                if (t1.stateAt(pos) == STATE_FILLED) {
                    V v = t1.valueAt(pos);
                    KeyHandle handle = KeyHandle.forFfm(t1.refs[pos], t1.hashAt(pos));
                    pos++;
                    if (!consumer.accept(handle, v)) {
                        return ScanCursorV2.ofPhaseAndPosition(phase, pos);
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
        rehashStep();
        int total = size();
        if (total == 0) {
            return null;
        }
        if (table1 == null) {
            return randomKeyFromTable(table0);
        }

        int r = ThreadLocalRandom.current().nextInt(total);
        byte[] k = r < table0.size ? randomKeyFromTable(table0) : randomKeyFromTable(table1);
        if (k != null) {
            return k;
        }
        return r < table0.size ? randomKeyFromTable(table1) : randomKeyFromTable(table0);
    }

    @Override
    public KeyHandle randomKeyHandle() {
        rehashStep();
        int total = size();
        if (total == 0) {
            return null;
        }
        if (table1 == null) {
            return randomKeyHandleFromTable(table0);
        }

        int r = ThreadLocalRandom.current().nextInt(total);
        KeyHandle handle = r < table0.size ? randomKeyHandleFromTable(table0) : randomKeyHandleFromTable(table1);
        if (handle != null) {
            return handle;
        }
        return r < table0.size ? randomKeyHandleFromTable(table1) : randomKeyHandleFromTable(table0);
    }

    private void ensureTable0() {
        if (table0 == null) {
            table0 = new Table(MIN_CAPACITY);
        }
    }

    private long tableBytes(Table table) {
        if (table == null) {
            return 0L;
        }
        return (long) table.statesRegion.size() + table.hashesRegion.size();
    }

    private void clearTable(Table table) {
        if (table == null) {
            return;
        }
        for (int i = 0; i < table.capacity; i++) {
            if (table.stateAt(i) != STATE_FILLED) {
                continue;
            }
            blobStore.release(table.refs[i]);
            table.refs[i] = null;
            table.values[i] = null;
        }
        table.close();
    }

    private void forEachTable(Table table, BiConsumer<byte[], V> consumer) {
        if (table == null) {
            return;
        }
        for (int i = 0; i < table.capacity; i++) {
            if (table.stateAt(i) != STATE_FILLED) {
                continue;
            }
            consumer.accept(blobStore.toByteArray(table.refs[i]), table.valueAt(i));
        }
    }

    private void forEachKeyHandleTable(Table table, BiConsumer<KeyHandle, V> consumer) {
        if (table == null) {
            return;
        }
        for (int i = 0; i < table.capacity; i++) {
            if (table.stateAt(i) != STATE_FILLED) {
                continue;
            }
            consumer.accept(KeyHandle.forFfm(table.refs[i], table.hashAt(i)), table.valueAt(i));
        }
    }

    private byte[] randomKeyFromTable(Table table) {
        if (table == null || table.capacity == 0) {
            return null;
        }
        int mask = table.capacity - 1;
        int start = ThreadLocalRandom.current().nextInt(table.capacity);

        int quickSteps = Math.min(16, table.capacity);
        for (int i = 0; i < quickSteps; i++) {
            int idx = (start + i) & mask;
            if (table.stateAt(idx) == STATE_FILLED) {
                return blobStore.toByteArray(table.refs[idx]);
            }
        }

        for (int i = 0; i < table.capacity; i++) {
            int idx = (start + i) & mask;
            if (table.stateAt(idx) == STATE_FILLED) {
                return blobStore.toByteArray(table.refs[idx]);
            }
        }
        return null;
    }

    private KeyHandle randomKeyHandleFromTable(Table table) {
        if (table == null || table.capacity == 0) {
            return null;
        }
        int mask = table.capacity - 1;
        int start = ThreadLocalRandom.current().nextInt(table.capacity);

        int quickSteps = Math.min(16, table.capacity);
        for (int i = 0; i < quickSteps; i++) {
            int idx = (start + i) & mask;
            if (table.stateAt(idx) == STATE_FILLED) {
                return KeyHandle.forFfm(table.refs[idx], table.hashAt(idx));
            }
        }

        for (int i = 0; i < table.capacity; i++) {
            int idx = (start + i) & mask;
            if (table.stateAt(idx) == STATE_FILLED) {
                return KeyHandle.forFfm(table.refs[idx], table.hashAt(idx));
            }
        }
        return null;
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
        startRehash(Math.max(target, MIN_CAPACITY));
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
            retireTable(t0);
            table0 = null;
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
        closeRetiredTables();
        if (table1 == null) {
            return;
        }
        int steps = 1;
        while (steps-- > 0 && table1 != null) {
            Table t0 = table0;
            while (rehashIndex < t0.capacity && t0.stateAt(rehashIndex) != STATE_FILLED) {
                rehashIndex++;
            }
            if (rehashIndex >= t0.capacity) {
                finishRehash();
                return;
            }

            int idx = rehashIndex;
            YierdisFfmBytesRef ref = t0.refs[idx];
            int hash = t0.hashAt(idx);
            V value = t0.valueAt(idx);

            t0.setState(idx, STATE_TOMBSTONE);
            t0.setHash(idx, 0);
            t0.refs[idx] = null;
            t0.values[idx] = null;
            t0.size--;

            insertExistingIntoTable1(ref, hash, value);
            rehashIndex++;
        }
    }

    private void finishRehash() {
        Table old0 = table0;
        Table new0 = table1;
        retireTable(old0);
        table0 = new0;
        table1 = null;
        rehashIndex = -1;
    }

    private void retireTable(Table table) {
        if (table != null) {
            retiredTables.addLast(table);
        }
    }

    private void closeRetiredTables() {
        while (!retiredTables.isEmpty()) {
            retiredTables.removeFirst().close();
        }
    }

    private void insertExistingIntoTable1(YierdisFfmBytesRef ref, int hash, V value) {
        Table t1 = table1;
        if (t1.used + 1 > t1.threshold) {
            growTable1();
            t1 = table1;
        }
        insertIntoTable(t1, ref, hash, value);
    }

    private void insertNew(YierdisFfmBytesRef ref, int hash, V value) {
        if (table1 != null) {
            insertExistingIntoTable1(ref, hash, value);
            return;
        }
        insertIntoTable(table0, ref, hash, value);
    }

    private void insertIntoTable(Table table, YierdisFfmBytesRef ref, int hash, V value) {
        int loc = findOrInsertLocation(table, ref, hash);
        int insertAt = -loc - 1;
        if (table.stateAt(insertAt) == STATE_EMPTY) {
            table.used++;
        }
        table.setState(insertAt, STATE_FILLED);
        table.setHash(insertAt, hash);
        table.refs[insertAt] = ref;
        table.values[insertAt] = value;
        table.size++;
    }

    private void growTable1() {
        Table old = table1;
        int oldSize = old.size;
        Table next = new Table(old.capacity << 1);
        for (int i = 0; i < old.capacity; i++) {
            if (old.stateAt(i) != STATE_FILLED) {
                continue;
            }
            YierdisFfmBytesRef ref = old.refs[i];
            int hash = old.hashAt(i);
            V value = old.valueAt(i);
            int loc = -findOrInsertLocation(next, ref, hash) - 1;
            next.setState(loc, STATE_FILLED);
            next.setHash(loc, hash);
            next.refs[loc] = ref;
            next.values[loc] = value;
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

    private Location findLocation(byte[] key, int hash) {
        int idx0 = findIndex(table0, key, hash);
        if (idx0 >= 0) {
            return new Location(0, idx0);
        }
        if (table1 != null) {
            int idx1 = findIndex(table1, key, hash);
            if (idx1 >= 0) {
                return new Location(1, idx1);
            }
        }
        return null;
    }

    private Location findLocation(KeyHandle keyHandle, int hash) {
        int idx0 = findIndex(table0, keyHandle, hash);
        if (idx0 >= 0) {
            return new Location(0, idx0);
        }
        if (table1 != null) {
            int idx1 = findIndex(table1, keyHandle, hash);
            if (idx1 >= 0) {
                return new Location(1, idx1);
            }
        }
        return null;
    }

    private int hash(byte[] key) {
        return mix(contentHash(key));
    }

    private int hash(BytesView key) {
        return mix(contentHash(key));
    }

    private int hash(KeyHandle keyHandle) {
        return mix(contentHash(keyHandle));
    }

    private static int contentHash(byte[] key) {
        int h = 1;
        for (byte b : key) {
            h = 31 * h + b;
        }
        return h;
    }

    private static int contentHash(BytesView key) {
        int len = key.length();
        if (len < 0) {
            throw new IllegalArgumentException("key length must be non-negative");
        }
        int h = 1;
        for (int i = 0; i < len; i++) {
            h = 31 * h + key.getByte(i);
        }
        return h;
    }

    private static int mix(int base) {
        int h = base;
        h ^= (h >>> 16);
        h *= 0x7feb352d;
        h ^= (h >>> 15);
        h *= 0x846ca68b;
        h ^= (h >>> 16);
        return h == 0 ? 1 : h;
    }

    private int findIndex(Table table, byte[] key, int hash) {
        if (table == null) {
            return -1;
        }
        int mask = table.capacity - 1;
        int idx = hash & mask;
        while (true) {
            byte state = table.stateAt(idx);
            if (state == STATE_EMPTY) {
                return -1;
            }
            if (state == STATE_FILLED && table.hashAt(idx) == hash && blobStore.equalsBytes(table.refs[idx], key)) {
                return idx;
            }
            idx = (idx + 1) & mask;
        }
    }

    private int findIndex(Table table, BytesView key, int hash) {
        if (table == null) {
            return -1;
        }
        int mask = table.capacity - 1;
        int idx = hash & mask;
        while (true) {
            byte state = table.stateAt(idx);
            if (state == STATE_EMPTY) {
                return -1;
            }
            if (state == STATE_FILLED && table.hashAt(idx) == hash && blobStore.equalsBytes(table.refs[idx], key)) {
                return idx;
            }
            idx = (idx + 1) & mask;
        }
    }

    private int findIndex(Table table, KeyHandle keyHandle, int hash) {
        if (table == null) {
            return -1;
        }
        YierdisFfmBytesRef handleRef = KeyHandleAccess.ffmBytesRefOrNull(keyHandle);
        int mask = table.capacity - 1;
        int idx = hash & mask;
        while (true) {
            byte state = table.stateAt(idx);
            if (state == STATE_EMPTY) {
                return -1;
            }
            if (state == STATE_FILLED && table.hashAt(idx) == hash) {
                YierdisFfmBytesRef storedRef = table.refs[idx];
                if (handleRef != null && sameRef(storedRef, handleRef)) {
                    return idx;
                }
                if (equalsBytes(storedRef, keyHandle)) {
                    return idx;
                }
            }
            idx = (idx + 1) & mask;
        }
    }

    private int findOrInsertLocation(Table table, YierdisFfmBytesRef ref, int hash) {
        int mask = table.capacity - 1;
        int idx = hash & mask;
        int firstTombstone = -1;
        while (true) {
            byte state = table.stateAt(idx);
            if (state == STATE_EMPTY) {
                int target = firstTombstone >= 0 ? firstTombstone : idx;
                return -(target + 1);
            }
            if (state == STATE_TOMBSTONE) {
                if (firstTombstone < 0) {
                    firstTombstone = idx;
                }
            } else if (table.hashAt(idx) == hash && sameRef(table.refs[idx], ref)) {
                return idx;
            }
            idx = (idx + 1) & mask;
        }
    }

    private boolean equalsBytes(YierdisFfmBytesRef storedRef, BytesView key) {
        int len = key.length();
        if (storedRef.length() != len) {
            return false;
        }
        YierdisFfmSpan span = storedRef.span();
        for (int i = 0; i < len; i++) {
            if (YierdisFfmAccess.getByte(span, i) != key.getByte(i)) {
                return false;
            }
        }
        return true;
    }

    private boolean sameRef(YierdisFfmBytesRef left, YierdisFfmBytesRef right) {
        return left == right
                || (left != null
                && right != null
                && left.region() == right.region()
                && left.offset() == right.offset()
                && left.length() == right.length());
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
        private final int table;
        private final int index;

        private Location(int table, int index) {
            this.table = table;
            this.index = index;
        }

        V getValue() {
            return currentTable().valueAt(index);
        }

        void setValue(V newValue) {
            currentTable().values[index] = newValue;
        }

        KeyHandle keyHandle() {
            Table current = currentTable();
            return KeyHandle.forFfm(current.refs[index], current.hashAt(index));
        }

        void remove() {
            Table current = currentTable();
            YierdisFfmBytesRef ref = current.refs[index];
            current.setState(index, STATE_TOMBSTONE);
            current.setHash(index, 0);
            current.refs[index] = null;
            current.values[index] = null;
            current.size--;
            blobStore.release(ref);
            if (table == 0) {
                maybeStartRehashForDeleteOrTombstones();
            }
        }

        private Table currentTable() {
            return table == 0 ? table0 : table1;
        }
    }

    private final class Table implements AutoCloseable {
        private final int capacity;
        private final YierdisFfmRegion statesRegion;
        private final YierdisFfmSpan states;
        private final YierdisFfmRegion hashesRegion;
        private final YierdisFfmSpan hashes;
        private final YierdisFfmBytesRef[] refs;
        private final Object[] values;

        private int size;
        private int used;
        private final int threshold;

        private Table(int capacity) {
            this.capacity = capacity;
            this.statesRegion = memoryRuntime.allocateRegion("ffm-keyspace-states", capacity);
            this.states = statesRegion.span(0, capacity);
            this.hashesRegion = memoryRuntime.allocateRegion("ffm-keyspace-hashes", capacity * Integer.BYTES);
            this.hashes = hashesRegion.span(0, capacity * Integer.BYTES);
            this.refs = new YierdisFfmBytesRef[capacity];
            this.values = new Object[capacity];
            this.threshold = (int) (capacity * LOAD_FACTOR);
        }

        private byte stateAt(int index) {
            return YierdisFfmAccess.getByte(states, index);
        }

        private void setState(int index, byte value) {
            YierdisFfmAccess.setByte(states, index, value);
        }

        private int hashAt(int index) {
            return YierdisFfmAccess.getInt(hashes, index * Integer.BYTES);
        }

        private void setHash(int index, int value) {
            YierdisFfmAccess.setInt(hashes, index * Integer.BYTES, value);
        }

        @SuppressWarnings("unchecked")
        private V valueAt(int index) {
            return (V) values[index];
        }

        @Override
        public void close() {
            statesRegion.close();
            hashesRegion.close();
        }
    }
}
