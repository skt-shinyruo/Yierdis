package yier.bubu.redis.db.offheap;

import yier.bubu.redis.db.YierdisBytesView;
import yier.bubu.redis.db.YierdisExpireIndex;
import yier.bubu.redis.db.YierdisKeyspace;
import yier.bubu.redis.db.offheap.unsafe.YierdisUnsafeAccess;
import yier.bubu.redis.db.offheap.unsafe.YierdisUnsafeOffHeapAllocator;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Unsafe off-heap TTL index mapping {@code key -> expireAtMillis}.
 * <p>
 * Keys are referenced by their off-heap {@code (addr,len)} owned by the main keyspace, i.e. this index does NOT
 * allocate or free key bytes. Slot arrays are stored off-heap.
 */
public final class YierdisUnsafeOffHeapExpireIndex implements YierdisExpireIndex {
    private static final float LOAD_FACTOR = 0.75f;
    private static final int MIN_CAPACITY = 16;
    private static final byte STATE_EMPTY = 0;
    private static final byte STATE_FILLED = 1;
    private static final byte STATE_TOMBSTONE = 2;

    private static final int INT_BYTES = Integer.BYTES;
    private static final int LONG_BYTES = Long.BYTES;
    private static final int ZERO_CHUNK_BYTES = 8 * 1024;
    private static final ThreadLocal<byte[]> TL_ZERO_CHUNK =
            ThreadLocal.withInitial(() -> new byte[ZERO_CHUNK_BYTES]);

    private final YierdisUnsafeOffHeapAllocator allocator;
    private final int seed;

    private Table table0;
    private Table table1;
    private int rehashIndex = -1;

    public YierdisUnsafeOffHeapExpireIndex(YierdisUnsafeOffHeapAllocator allocator) {
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
    public Long get(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Table t0 = table0;
        if (t0 == null) {
            return null;
        }
        rehashStep();
        int h = hash(keyBytes);
        int idx = findIndex(t0, keyBytes, h);
        if (idx >= 0) {
            return getLong(t0.expireAtAddr, idx);
        }
        Table t1 = table1;
        if (t1 != null) {
            idx = findIndex(t1, keyBytes, h);
            if (idx >= 0) {
                return getLong(t1.expireAtAddr, idx);
            }
        }
        return null;
    }

    @Override
    public Long get(YierdisBytesView keyView) {
        Objects.requireNonNull(keyView, "keyView");
        Table t0 = table0;
        if (t0 == null) {
            return null;
        }
        rehashStep();
        int h = hash(keyView);
        int idx = findIndex(t0, keyView, h);
        if (idx >= 0) {
            return getLong(t0.expireAtAddr, idx);
        }
        Table t1 = table1;
        if (t1 != null) {
            idx = findIndex(t1, keyView, h);
            if (idx >= 0) {
                return getLong(t1.expireAtAddr, idx);
            }
        }
        return null;
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
    public void clear() {
        Table t0 = table0;
        if (t0 != null) {
            t0.close();
        }
        Table t1 = table1;
        if (t1 != null) {
            t1.close();
        }
        table0 = null;
        table1 = null;
        rehashIndex = -1;
    }

    @Override
    public void setExpireAtMillis(byte[] keyBytes, long expireAtMillis, YierdisKeyspace<?> store) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Objects.requireNonNull(store, "store");

        if (!(store instanceof YierdisUnsafeOffHeapKeyspace<?> unsafeStore)) {
            throw new IllegalArgumentException("store must be YierdisUnsafeOffHeapKeyspace");
        }

        YierdisUnsafeOffHeapKeyspace.KeyHandle handle = unsafeStore.keyHandle(keyBytes);
        if (handle == null) {
            return;
        }

        ensureTable0();
        rehashStep();
        maybeStartRehashForInsert();

        int h = hash(keyBytes);
        long keyPtr = handle.keyPtr;
        int keyLen = handle.keyLen;

        int idx = findIndexByPtr(table0, keyPtr, keyLen, h);
        if (idx >= 0) {
            putLong(table0.expireAtAddr, idx, expireAtMillis);
            return;
        }
        Table t1 = table1;
        if (t1 != null) {
            idx = findIndexByPtr(t1, keyPtr, keyLen, h);
            if (idx >= 0) {
                putLong(t1.expireAtAddr, idx, expireAtMillis);
                return;
            }
        }

        if (t1 != null) {
            if (t1.used + 1 > t1.threshold) {
                growTable1();
                t1 = table1;
            }
            insertIntoTable(t1, keyPtr, keyLen, h, expireAtMillis);
            return;
        }
        insertIntoTable(table0, keyPtr, keyLen, h, expireAtMillis);
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
        if (loc == null) {
            return;
        }
        loc.remove();
    }

    private void ensureTable0() {
        if (table0 != null) {
            return;
        }
        table0 = new Table(MIN_CAPACITY);
    }

    private Location findLocation(byte[] keyBytes, int hash) {
        Table t0 = table0;
        int idx0 = findIndex(t0, keyBytes, hash);
        if (idx0 >= 0) {
            return new Location(0, idx0);
        }
        Table t1 = table1;
        if (t1 != null) {
            int idx1 = findIndex(t1, keyBytes, hash);
            if (idx1 >= 0) {
                return new Location(1, idx1);
            }
        }
        return null;
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
        byte[] out = new byte[keyLen];
        YierdisUnsafeAccess.copyMemory(keyPtr, out, 0, keyLen);
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
            long expireAt = getLong(t0.expireAtAddr, idx);

            putByte(t0.statesAddr, idx, STATE_TOMBSTONE);
            putLong(t0.keyPtrAddr, idx, 0L);
            putInt(t0.keyLenAddr, idx, 0);
            putInt(t0.hashesAddr, idx, 0);
            putLong(t0.expireAtAddr, idx, 0L);
            t0.size--;

            insertExistingIntoTable1(keyPtr, keyLen, hash, expireAt);

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

    private void insertExistingIntoTable1(long keyPtr, int keyLen, int hash, long expireAtMillis) {
        Table t1 = table1;
        if (t1.used + 1 > t1.threshold) {
            growTable1();
            t1 = table1;
        }
        insertIntoTable(t1, keyPtr, keyLen, hash, expireAtMillis);
    }

    private void insertIntoTable(Table t, long keyPtr, int keyLen, int hash, long expireAtMillis) {
        int loc = findOrInsertLocation(t, keyPtr, keyLen, hash);
        int insertAt = -loc - 1;
        if (getByte(t.statesAddr, insertAt) == STATE_EMPTY) {
            t.used++;
        }
        putByte(t.statesAddr, insertAt, STATE_FILLED);
        putInt(t.hashesAddr, insertAt, hash);
        putLong(t.keyPtrAddr, insertAt, keyPtr);
        putInt(t.keyLenAddr, insertAt, keyLen);
        putLong(t.expireAtAddr, insertAt, expireAtMillis);
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
            long expireAt = getLong(old.expireAtAddr, i);

            int loc = -findOrInsertLocation(next, keyPtr, keyLen, hash) - 1;
            putByte(next.statesAddr, loc, STATE_FILLED);
            putInt(next.hashesAddr, loc, hash);
            putLong(next.keyPtrAddr, loc, keyPtr);
            putInt(next.keyLenAddr, loc, keyLen);
            putLong(next.expireAtAddr, loc, expireAt);
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

    private static boolean equalsKey(long storedPtr, byte[] key, int len) {
        for (int i = 0; i < len; i++) {
            if (YierdisUnsafeAccess.getByte(storedPtr + i) != key[i]) {
                return false;
            }
        }
        return true;
    }

    private static boolean equalsKey(long storedPtr, YierdisBytesView key, int len) {
        for (int i = 0; i < len; i++) {
            if (YierdisUnsafeAccess.getByte(storedPtr + i) != key.byteAt(i)) {
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

    private static void clearMemory(long address, int bytes) {
        if (bytes <= 0) {
            return;
        }
        byte[] zeros = TL_ZERO_CHUNK.get();
        int remaining = bytes;
        long dst = address;
        while (remaining > 0) {
            int chunk = Math.min(remaining, zeros.length);
            YierdisUnsafeAccess.copyMemory(zeros, 0, dst, chunk);
            dst += chunk;
            remaining -= chunk;
        }
    }

    private static byte getByte(long base, int index) {
        return YierdisUnsafeAccess.getByte(base + index);
    }

    private static void putByte(long base, int index, byte value) {
        YierdisUnsafeAccess.putByte(base + index, value);
    }

    private static int getInt(long base, int index) {
        long addr = base + (long) index * INT_BYTES;
        int b0 = YierdisUnsafeAccess.getByte(addr) & 0xff;
        int b1 = YierdisUnsafeAccess.getByte(addr + 1) & 0xff;
        int b2 = YierdisUnsafeAccess.getByte(addr + 2) & 0xff;
        int b3 = YierdisUnsafeAccess.getByte(addr + 3) & 0xff;
        return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
    }

    private static void putInt(long base, int index, int value) {
        long addr = base + (long) index * INT_BYTES;
        YierdisUnsafeAccess.putByte(addr, (byte) value);
        YierdisUnsafeAccess.putByte(addr + 1, (byte) (value >>> 8));
        YierdisUnsafeAccess.putByte(addr + 2, (byte) (value >>> 16));
        YierdisUnsafeAccess.putByte(addr + 3, (byte) (value >>> 24));
    }

    private static long getLong(long base, int index) {
        long addr = base + (long) index * LONG_BYTES;
        long b0 = YierdisUnsafeAccess.getByte(addr) & 0xffL;
        long b1 = YierdisUnsafeAccess.getByte(addr + 1) & 0xffL;
        long b2 = YierdisUnsafeAccess.getByte(addr + 2) & 0xffL;
        long b3 = YierdisUnsafeAccess.getByte(addr + 3) & 0xffL;
        long b4 = YierdisUnsafeAccess.getByte(addr + 4) & 0xffL;
        long b5 = YierdisUnsafeAccess.getByte(addr + 5) & 0xffL;
        long b6 = YierdisUnsafeAccess.getByte(addr + 6) & 0xffL;
        long b7 = YierdisUnsafeAccess.getByte(addr + 7) & 0xffL;
        return b0
                | (b1 << 8)
                | (b2 << 16)
                | (b3 << 24)
                | (b4 << 32)
                | (b5 << 40)
                | (b6 << 48)
                | (b7 << 56);
    }

    private static void putLong(long base, int index, long value) {
        long addr = base + (long) index * LONG_BYTES;
        YierdisUnsafeAccess.putByte(addr, (byte) value);
        YierdisUnsafeAccess.putByte(addr + 1, (byte) (value >>> 8));
        YierdisUnsafeAccess.putByte(addr + 2, (byte) (value >>> 16));
        YierdisUnsafeAccess.putByte(addr + 3, (byte) (value >>> 24));
        YierdisUnsafeAccess.putByte(addr + 4, (byte) (value >>> 32));
        YierdisUnsafeAccess.putByte(addr + 5, (byte) (value >>> 40));
        YierdisUnsafeAccess.putByte(addr + 6, (byte) (value >>> 48));
        YierdisUnsafeAccess.putByte(addr + 7, (byte) (value >>> 56));
    }

    private final class Location {
        private final int table; // 0 or 1
        private final int index;

        private Location(int table, int index) {
            this.table = table;
            this.index = index;
        }

        void remove() {
            Table t = table == 0 ? table0 : table1;
            putByte(t.statesAddr, index, STATE_TOMBSTONE);
            putLong(t.keyPtrAddr, index, 0L);
            putInt(t.keyLenAddr, index, 0);
            putInt(t.hashesAddr, index, 0);
            putLong(t.expireAtAddr, index, 0L);
            t.size--;

            if (table == 0) {
                maybeStartRehashForDeleteOrTombstones();
            }
        }
    }

    private final class Table {
        final int capacity;
        final YierdisUnsafeOffHeapAllocator.YierdisUnsafeOffHeapBlock statesBlock;
        final YierdisUnsafeOffHeapAllocator.YierdisUnsafeOffHeapBlock hashesBlock;
        final YierdisUnsafeOffHeapAllocator.YierdisUnsafeOffHeapBlock keyPtrBlock;
        final YierdisUnsafeOffHeapAllocator.YierdisUnsafeOffHeapBlock keyLenBlock;
        final YierdisUnsafeOffHeapAllocator.YierdisUnsafeOffHeapBlock expireAtBlock;

        final long statesAddr;
        final long hashesAddr;
        final long keyPtrAddr;
        final long keyLenAddr;
        final long expireAtAddr;

        int size;
        int used;
        final int threshold;

        Table(int capacity) {
            this.capacity = capacity;
            this.statesBlock = allocator.allocateBlock(capacity);
            this.hashesBlock = allocator.allocateBlock(capacity * INT_BYTES);
            this.keyPtrBlock = allocator.allocateBlock(capacity * LONG_BYTES);
            this.keyLenBlock = allocator.allocateBlock(capacity * INT_BYTES);
            this.expireAtBlock = allocator.allocateBlock(capacity * LONG_BYTES);

            this.statesAddr = statesBlock.address();
            this.hashesAddr = hashesBlock.address();
            this.keyPtrAddr = keyPtrBlock.address();
            this.keyLenAddr = keyLenBlock.address();
            this.expireAtAddr = expireAtBlock.address();

            clearMemory(statesAddr, capacity);
            clearMemory(hashesAddr, capacity * INT_BYTES);
            clearMemory(keyPtrAddr, capacity * LONG_BYTES);
            clearMemory(keyLenAddr, capacity * INT_BYTES);
            clearMemory(expireAtAddr, capacity * LONG_BYTES);

            this.threshold = (int) (capacity * LOAD_FACTOR);
        }

        void close() {
            statesBlock.close();
            hashesBlock.close();
            keyPtrBlock.close();
            keyLenBlock.close();
            expireAtBlock.close();
        }
    }
}
