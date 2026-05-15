package yier.bubu.redis.storage.memory.internal.ffm;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.memory.internal.expire.YierdisExpireIndex;
import yier.bubu.redis.storage.memory.internal.keyspace.YierdisKeyspace;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandleAccess;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.foreign.YierdisFfmAccess;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisFfmRegion;
import yier.bubu.redis.memory.foreign.YierdisFfmSpan;

import java.util.Objects;
import java.util.ArrayDeque;
import java.util.concurrent.ThreadLocalRandom;

public final class YierdisFfmExpireIndex implements YierdisExpireIndex {
    private static final float LOAD_FACTOR = 0.75f;
    private static final int MIN_CAPACITY = 16;
    private static final byte STATE_EMPTY = 0;
    private static final byte STATE_FILLED = 1;
    private static final byte STATE_TOMBSTONE = 2;

    private final YierdisFfmBlobStore blobStore;
    private final YierdisFfmMemoryRuntime memoryRuntime;
    private final NativeAllocator nativeAllocator;
    private final ArrayDeque<Table> retiredTables = new ArrayDeque<>();

    private Table table0;
    private Table table1;
    private int rehashIndex = -1;

    public YierdisFfmExpireIndex(YierdisFfmBlobStore blobStore) {
        this.blobStore = Objects.requireNonNull(blobStore, "blobStore");
        this.memoryRuntime = blobStore.memoryRuntime();
        this.nativeAllocator = null;
    }

    public YierdisFfmExpireIndex(YierdisFfmMemoryRuntime memoryRuntime, NativeAllocator nativeAllocator) {
        this.blobStore = null;
        this.memoryRuntime = Objects.requireNonNull(memoryRuntime, "memoryRuntime");
        this.nativeAllocator = Objects.requireNonNull(nativeAllocator, "nativeAllocator");
    }

    @Override
    public int size() {
        closeRetiredTables();
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
        return estimatedTableOverheadBytes();
    }

    @Override
    public Long get(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        rehashStep();
        Table t0 = table0;
        if (t0 == null) {
            return null;
        }
        int h = hash(keyBytes);
        int idx = findIndex(t0, keyBytes, h);
        if (idx >= 0) {
            return t0.expireAt(idx);
        }
        Table t1 = table1;
        if (t1 != null) {
            idx = findIndex(t1, keyBytes, h);
            if (idx >= 0) {
                return t1.expireAt(idx);
            }
        }
        return null;
    }

    @Override
    public Long get(BytesView keyView) {
        Objects.requireNonNull(keyView, "keyView");
        rehashStep();
        Table t0 = table0;
        if (t0 == null) {
            return null;
        }
        int h = hash(keyView);
        int idx = findIndex(t0, keyView, h);
        if (idx >= 0) {
            return t0.expireAt(idx);
        }
        Table t1 = table1;
        if (t1 != null) {
            idx = findIndex(t1, keyView, h);
            if (idx >= 0) {
                return t1.expireAt(idx);
            }
        }
        return null;
    }

    @Override
    public Long get(KeyHandle keyHandle) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        rehashStep();
        Table t0 = table0;
        if (t0 == null) {
            return null;
        }
        int h = hash(keyHandle);
        int idx = findIndex(t0, keyHandle, h);
        if (idx >= 0) {
            return t0.expireAt(idx);
        }
        Table t1 = table1;
        if (t1 != null) {
            idx = findIndex(t1, keyHandle, h);
            if (idx >= 0) {
                return t1.expireAt(idx);
            }
        }
        return null;
    }

    @Override
    public byte[] randomKey() {
        KeyHandle handle = randomKeyHandle();
        return handle == null ? null : copyKey(handle);
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
    public void setExpireAtMillis(byte[] keyBytes, long expireAtMillis, YierdisKeyspace<?> store) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Objects.requireNonNull(store, "store");
        KeyHandle handle = store.keyHandle(keyBytes);
        if (handle == null) {
            return;
        }
        setExpireAtMillis(handle, expireAtMillis);
    }

    @Override
    public void setExpireAtMillis(KeyHandle keyHandle, long expireAtMillis) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        KeyRef ref = keyRef(keyHandle);
        ensureTable0();
        rehashStep();
        maybeStartRehashForInsert();

        int h = hash(keyHandle);
        int idx = findIndex(table0, keyHandle, h);
        if (idx >= 0) {
            table0.setExpireAt(idx, expireAtMillis);
            return;
        }
        Table t1 = table1;
        if (t1 != null) {
            idx = findIndex(t1, keyHandle, h);
            if (idx >= 0) {
                t1.setExpireAt(idx, expireAtMillis);
                return;
            }
        }

        if (table1 != null) {
            insertNewIntoTable1(ref, h, expireAtMillis);
            return;
        }
        insertNewIntoTable(table0, ref, h, expireAtMillis);
    }

    @Override
    public void removeExpire(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Table t0 = table0;
        if (t0 == null) {
            return;
        }
        rehashStep();
        int h = hash(keyBytes);
        Location loc = findLocation(keyBytes, h);
        if (loc != null) {
            loc.remove();
        }
    }

    @Override
    public void removeExpire(KeyHandle keyHandle) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        Table t0 = table0;
        if (t0 == null) {
            return;
        }
        rehashStep();
        int h = hash(keyHandle);
        Location loc = findLocation(keyHandle, h);
        if (loc != null) {
            loc.remove();
        }
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
        return (long) table.statesRegion.size() + table.hashesRegion.size() + table.expireAtRegion.size();
    }

    private void clearTable(Table table) {
        if (table == null) {
            return;
        }
        for (int i = 0; i < table.capacity; i++) {
            if (table.stateAt(i) != STATE_FILLED) {
                continue;
            }
            table.refs[i].releaseFromIndex();
            table.refs[i] = null;
        }
        table.close();
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
                return table.refs[idx].keyHandle(table.hashAt(idx));
            }
        }
        for (int i = 0; i < table.capacity; i++) {
            int idx = (start + i) & mask;
            if (table.stateAt(idx) == STATE_FILLED) {
                return table.refs[idx].keyHandle(table.hashAt(idx));
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
            KeyRef ref = t0.refs[idx];
            int hash = t0.hashAt(idx);
            long expireAt = t0.expireAt(idx);

            t0.setState(idx, STATE_TOMBSTONE);
            t0.setHash(idx, 0);
            t0.setExpireAt(idx, 0L);
            t0.refs[idx] = null;
            t0.size--;

            insertExistingIntoTable1(ref, hash, expireAt);
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

    private void insertExistingIntoTable1(KeyRef ref, int hash, long expireAtMillis) {
        Table t1 = table1;
        if (t1.used + 1 > t1.threshold) {
            growTable1();
            t1 = table1;
        }
        insertIntoTable(t1, ref, hash, expireAtMillis);
    }

    private void insertNewIntoTable1(KeyRef ref, int hash, long expireAtMillis) {
        Table t1 = table1;
        if (t1.used + 1 > t1.threshold) {
            growTable1();
            t1 = table1;
        }
        insertNewIntoTable(t1, ref, hash, expireAtMillis);
    }

    private void insertNewIntoTable(Table table, KeyRef ref, int hash, long expireAtMillis) {
        ref.retainForIndex();
        insertIntoTable(table, ref, hash, expireAtMillis);
    }

    private void insertIntoTable(Table table, KeyRef ref, int hash, long expireAtMillis) {
        int loc = findOrInsertLocation(table, ref, hash);
        int insertAt = -loc - 1;
        if (table.stateAt(insertAt) == STATE_EMPTY) {
            table.used++;
        }
        table.setState(insertAt, STATE_FILLED);
        table.setHash(insertAt, hash);
        table.refs[insertAt] = ref;
        table.setExpireAt(insertAt, expireAtMillis);
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
            KeyRef ref = old.refs[i];
            int hash = old.hashAt(i);
            long expireAt = old.expireAt(i);
            int loc = -findOrInsertLocation(next, ref, hash) - 1;
            next.setState(loc, STATE_FILLED);
            next.setHash(loc, hash);
            next.refs[loc] = ref;
            next.setExpireAt(loc, expireAt);
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
            if (state == STATE_FILLED && table.hashAt(idx) == hash && table.refs[idx].equalsBytes(key)) {
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
            if (state == STATE_FILLED && table.hashAt(idx) == hash && table.refs[idx].equalsBytes(key)) {
                return idx;
            }
            idx = (idx + 1) & mask;
        }
    }

    private int findIndex(Table table, KeyHandle keyHandle, int hash) {
        if (table == null) {
            return -1;
        }
        KeyRef handleRef = keyRefOrNull(keyHandle);
        int mask = table.capacity - 1;
        int idx = hash & mask;
        while (true) {
            byte state = table.stateAt(idx);
            if (state == STATE_EMPTY) {
                return -1;
            }
            if (state == STATE_FILLED && table.hashAt(idx) == hash) {
                KeyRef storedRef = table.refs[idx];
                if (handleRef != null && sameRef(storedRef, handleRef)) {
                    return idx;
                }
                if (storedRef.equalsBytes(keyHandle)) {
                    return idx;
                }
            }
            idx = (idx + 1) & mask;
        }
    }

    private int findOrInsertLocation(Table table, KeyRef ref, int hash) {
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

    private boolean sameRef(KeyRef left, KeyRef right) {
        return left != null && left.sameIdentity(right);
    }

    private KeyRef keyRef(KeyHandle handle) {
        KeyRef ref = keyRefOrNull(handle);
        if (ref == null) {
            throw new IllegalArgumentException("unsupported KeyHandle: " + handle.getClass().getName());
        }
        return ref;
    }

    private KeyRef keyRefOrNull(KeyHandle handle) {
        YierdisFfmBytesRef ffm = KeyHandleAccess.ffmBytesRefOrNull(handle);
        if (ffm != null) {
            return new FfmKeyRef(ffm);
        }
        NativeHandle nativeHandle = KeyHandleAccess.allocatorNativeHandleOrNull(handle);
        if (nativeHandle != null) {
            return new AllocatorKeyRef(nativeHandle);
        }
        return null;
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

        void remove() {
            Table current = table == 0 ? table0 : table1;
            KeyRef ref = current.refs[index];
            current.setState(index, STATE_TOMBSTONE);
            current.setHash(index, 0);
            current.setExpireAt(index, 0L);
            current.refs[index] = null;
            current.size--;
            if (ref != null) {
                ref.releaseFromIndex();
            }
            if (table == 0) {
                maybeStartRehashForDeleteOrTombstones();
            }
        }
    }

    private final class Table implements AutoCloseable {
        private final int capacity;
        private final YierdisFfmRegion statesRegion;
        private final YierdisFfmSpan states;
        private final YierdisFfmRegion hashesRegion;
        private final YierdisFfmSpan hashes;
        private final YierdisFfmRegion expireAtRegion;
        private final YierdisFfmSpan expireAt;
        private final KeyRef[] refs;

        private int size;
        private int used;
        private final int threshold;

        private Table(int capacity) {
            this.capacity = capacity;
            this.statesRegion = memoryRuntime.allocateRegion("ffm-expire-states", capacity);
            this.states = statesRegion.span(0, capacity);
            this.hashesRegion = memoryRuntime.allocateRegion("ffm-expire-hashes", capacity * Integer.BYTES);
            this.hashes = hashesRegion.span(0, capacity * Integer.BYTES);
            this.expireAtRegion = memoryRuntime.allocateRegion("ffm-expire-values", capacity * Long.BYTES);
            this.expireAt = expireAtRegion.span(0, capacity * Long.BYTES);
            this.refs = new KeyRef[capacity];
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

        private long expireAt(int index) {
            return YierdisFfmAccess.getLong(expireAt, index * Long.BYTES);
        }

        private void setExpireAt(int index, long value) {
            YierdisFfmAccess.setLong(expireAt, index * Long.BYTES, value);
        }

        @Override
        public void close() {
            statesRegion.close();
            hashesRegion.close();
            expireAtRegion.close();
        }
    }

    private static byte[] copyKey(KeyHandle keyHandle) {
        byte[] out = new byte[keyHandle.len()];
        for (int i = 0; i < out.length; i++) {
            out[i] = keyHandle.byteAt(i);
        }
        return out;
    }

    private sealed interface KeyRef permits FfmKeyRef, AllocatorKeyRef {
        boolean equalsBytes(byte[] key);

        boolean equalsBytes(BytesView key);

        KeyHandle keyHandle(int hash);

        boolean sameIdentity(KeyRef other);

        void retainForIndex();

        void releaseFromIndex();
    }

    private final class FfmKeyRef implements KeyRef {
        private final YierdisFfmBytesRef ref;

        private FfmKeyRef(YierdisFfmBytesRef ref) {
            this.ref = Objects.requireNonNull(ref, "ref");
        }

        @Override
        public boolean equalsBytes(byte[] key) {
            if (key.length != ref.length()) {
                return false;
            }
            for (int i = 0; i < ref.length(); i++) {
                if (ref.byteAt(i) != key[i]) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean equalsBytes(BytesView key) {
            if (key.length() != ref.length()) {
                return false;
            }
            for (int i = 0; i < ref.length(); i++) {
                if (ref.byteAt(i) != key.getByte(i)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public KeyHandle keyHandle(int hash) {
            return KeyHandle.forFfm(ref, hash);
        }

        @Override
        public boolean sameIdentity(KeyRef other) {
            return other instanceof FfmKeyRef f
                    && (ref == f.ref
                    || (ref.region() == f.ref.region()
                    && ref.offset() == f.ref.offset()
                    && ref.length() == f.ref.length()));
        }

        @Override
        public void retainForIndex() {
            if (blobStore != null) {
                blobStore.retain(ref);
            }
        }

        @Override
        public void releaseFromIndex() {
            if (blobStore != null) {
                blobStore.release(ref);
            }
        }
    }

    private final class AllocatorKeyRef implements KeyRef {
        private final NativeHandle handle;

        private AllocatorKeyRef(NativeHandle handle) {
            this.handle = Objects.requireNonNull(handle, "handle");
        }

        @Override
        public boolean equalsBytes(byte[] key) {
            KeyHandle stored = keyHandle(0);
            if (stored.len() != key.length) {
                return false;
            }
            for (int i = 0; i < key.length; i++) {
                if (stored.byteAt(i) != key[i]) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean equalsBytes(BytesView key) {
            KeyHandle stored = keyHandle(0);
            int len = key.length();
            if (stored.len() != len) {
                return false;
            }
            for (int i = 0; i < len; i++) {
                if (stored.byteAt(i) != key.getByte(i)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public KeyHandle keyHandle(int hash) {
            return KeyHandle.forNative(nativeAllocator, handle, hash);
        }

        @Override
        public boolean sameIdentity(KeyRef other) {
            return other instanceof AllocatorKeyRef a && handle.equals(a.handle);
        }

        @Override
        public void retainForIndex() {
        }

        @Override
        public void releaseFromIndex() {
        }
    }
}
