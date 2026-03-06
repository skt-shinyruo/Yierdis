package yier.bubu.redis.db.offheap;

import yier.bubu.redis.offheap.api.OffHeapAddressAllocator;
import yier.bubu.redis.offheap.api.OffHeapBlock;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * An Unsafe off-heap open-addressing hash table mapping {@code byte[]} keys to a {@code long} value.
 * <p>
 * Keys and all slot arrays are stored off-heap. The stored {@code long} value is also stored off-heap.
 * <p>
 * Ownership:
 * <ul>
 *   <li>The table OWNS key byte blocks and frees them on remove/clear.</li>
 *   <li>The table does NOT own the stored {@code long} values. Callers decide what they represent.</li>
 * </ul>
 */
public final class YierdisUnsafeOffHeapDictLong implements AutoCloseable {
    private static final float LOAD_FACTOR = 0.75f;
    private static final int MIN_CAPACITY = 16;
    private static final byte STATE_EMPTY = 0;
    private static final byte STATE_FILLED = 1;
    private static final byte STATE_TOMBSTONE = 2;

    private static final int INT_BYTES = Integer.BYTES;
    private static final int LONG_BYTES = Long.BYTES;

    private final OffHeapAddressAllocator allocator;
    private final int seed;

    private Table table;

    public YierdisUnsafeOffHeapDictLong(OffHeapAddressAllocator allocator) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.seed = ThreadLocalRandom.current().nextInt();
    }

    public int size() {
        Table t = table;
        return t == null ? 0 : t.size;
    }

    public long get(byte[] key) {
        Objects.requireNonNull(key, "key");
        Table t = table;
        if (t == null || t.size == 0) {
            return 0L;
        }
        int h = hash(key);
        int idx = findIndex(t, key, h);
        if (idx < 0) {
            return 0L;
        }
        return getLong(t.valuesAddr, idx);
    }

    public KeyHandle keyHandle(byte[] key) {
        Objects.requireNonNull(key, "key");
        Table t = table;
        if (t == null || t.size == 0) {
            return null;
        }
        int h = hash(key);
        int idx = findIndex(t, key, h);
        if (idx < 0) {
            return null;
        }
        return new KeyHandle(getLong(t.keyPtrAddr, idx), getInt(t.keyLenAddr, idx), h);
    }

    /**
     * Puts {@code key -> value} and returns the previous value, or {@code 0} if absent.
     */
    public long put(byte[] key, long value) {
        Objects.requireNonNull(key, "key");
        ensureTable();
        Table t = table;
        maybeResizeForInsert(t);
        // maybeResizeForInsert may rehash and replace the table instance.
        t = table;

        int h = hash(key);
        int mask = t.capacity - 1;
        int idx = h & mask;

        int firstTombstone = -1;
        while (true) {
            byte state = getByte(t.statesAddr, idx);
            if (state == STATE_EMPTY) {
                int insertIdx = firstTombstone >= 0 ? firstTombstone : idx;
                boolean insertingIntoEmpty = firstTombstone < 0;
                insertNew(t, insertIdx, key, h, value, insertingIntoEmpty);
                return 0L;
            }
            if (state == STATE_TOMBSTONE) {
                if (firstTombstone < 0) {
                    firstTombstone = idx;
                }
            } else if (getInt(t.hashesAddr, idx) == h) {
                int keyLen = getInt(t.keyLenAddr, idx);
                if (keyLen == key.length) {
                    long keyPtr = getLong(t.keyPtrAddr, idx);
                    if (equalsKey(keyPtr, key, keyLen)) {
                        long old = getLong(t.valuesAddr, idx);
                        putLong(t.valuesAddr, idx, value);
                        return old;
                    }
                }
            }
            idx = (idx + 1) & mask;
        }
    }

    /**
     * Removes {@code key} and returns the previous value, or {@code 0} if absent.
     */
    public long remove(byte[] key) {
        Objects.requireNonNull(key, "key");
        Table t = table;
        if (t == null || t.size == 0) {
            return 0L;
        }
        int h = hash(key);
        int idx = findIndex(t, key, h);
        if (idx < 0) {
            return 0L;
        }

        long old = getLong(t.valuesAddr, idx);
        freeKeyAt(t, idx);
        putByte(t.statesAddr, idx, STATE_TOMBSTONE);
        putInt(t.hashesAddr, idx, 0);
        putInt(t.keyLenAddr, idx, 0);
        putLong(t.keyPtrAddr, idx, 0L);
        putLong(t.valuesAddr, idx, 0L);
        t.size--;

        maybeRehashForTombstones(t);
        return old;
    }

    /**
     * Removes an entry by its stored off-heap key pointer/length/hash and returns the previous value, or {@code 0} if absent.
     * <p>
     * This avoids allocating a temporary heap {@code byte[]} when callers already hold a canonical key handle.
     */
    public long removeByPtr(long keyPtr, int keyLen, int hash) {
        if (keyLen < 0) {
            throw new IllegalArgumentException("keyLen must be >= 0");
        }
        Table t = table;
        if (t == null || t.size == 0) {
            return 0L;
        }

        int mask = t.capacity - 1;
        int idx = hash & mask;
        while (true) {
            byte state = getByte(t.statesAddr, idx);
            if (state == STATE_EMPTY) {
                return 0L;
            }
            if (state == STATE_FILLED && getInt(t.hashesAddr, idx) == hash) {
                if (getInt(t.keyLenAddr, idx) == keyLen && getLong(t.keyPtrAddr, idx) == keyPtr) {
                    long old = getLong(t.valuesAddr, idx);
                    freeKeyAt(t, idx);
                    putByte(t.statesAddr, idx, STATE_TOMBSTONE);
                    putInt(t.hashesAddr, idx, 0);
                    putInt(t.keyLenAddr, idx, 0);
                    putLong(t.keyPtrAddr, idx, 0L);
                    putLong(t.valuesAddr, idx, 0L);
                    t.size--;

                    maybeRehashForTombstones(t);
                    return old;
                }
            }
            idx = (idx + 1) & mask;
        }
    }

    public void forEach(EntryConsumer consumer) {
        Objects.requireNonNull(consumer, "consumer");
        Table t = table;
        if (t == null || t.size == 0) {
            return;
        }
        for (int i = 0; i < t.capacity; i++) {
            if (getByte(t.statesAddr, i) != STATE_FILLED) {
                continue;
            }
            long keyPtr = getLong(t.keyPtrAddr, i);
            int keyLen = getInt(t.keyLenAddr, i);
            long value = getLong(t.valuesAddr, i);
            consumer.accept(keyPtr, keyLen, value);
        }
    }

    public void clear() {
        Table t = table;
        if (t == null) {
            return;
        }
        for (int i = 0; i < t.capacity; i++) {
            if (getByte(t.statesAddr, i) != STATE_FILLED) {
                continue;
            }
            freeKeyAt(t, i);
        }
        t.close();
        table = null;
    }

    @Override
    public void close() {
        clear();
    }

    private void ensureTable() {
        if (table != null) {
            return;
        }
        table = new Table(MIN_CAPACITY);
    }

    private void maybeResizeForInsert(Table t) {
        if (t.used + 1 <= t.threshold) {
            return;
        }
        int target = tableSizeFor(t.size + 1, LOAD_FACTOR);
        if (target < MIN_CAPACITY) {
            target = MIN_CAPACITY;
        }
        rehashTo(target);
    }

    private void maybeRehashForTombstones(Table t) {
        // If tombstones dominate, rebuild the table at the same capacity.
        int tombstones = t.used - t.size;
        if (tombstones <= 0) {
            return;
        }
        if (t.used < t.threshold) {
            return;
        }
        if (tombstones < t.size) {
            return;
        }
        rehashTo(t.capacity);
    }

    private void rehashTo(int newCapacity) {
        Table old = table;
        Table next = new Table(newCapacity);
        try {
            for (int i = 0; i < old.capacity; i++) {
                if (getByte(old.statesAddr, i) != STATE_FILLED) {
                    continue;
                }
                long keyPtr = getLong(old.keyPtrAddr, i);
                int keyLen = getInt(old.keyLenAddr, i);
                int h = getInt(old.hashesAddr, i);
                long value = getLong(old.valuesAddr, i);

                insertExisting(next, keyPtr, keyLen, h, value);
            }
        } catch (RuntimeException e) {
            next.close();
            throw e;
        }
        // At this point, "next" owns all key blocks and we must prevent old from freeing them.
        for (int i = 0; i < old.capacity; i++) {
            putByte(old.statesAddr, i, STATE_EMPTY);
            putLong(old.keyPtrAddr, i, 0L);
            putInt(old.keyLenAddr, i, 0);
            putInt(old.hashesAddr, i, 0);
            putLong(old.valuesAddr, i, 0L);
        }
        old.size = 0;
        old.used = 0;

        old.close();
        table = next;
    }

    private void insertNew(Table t, int idx, byte[] key, int hash, long value, boolean intoEmptySlot) {
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

        putByte(t.statesAddr, idx, STATE_FILLED);
        putInt(t.hashesAddr, idx, hash);
        putInt(t.keyLenAddr, idx, keyLen);
        putLong(t.keyPtrAddr, idx, keyPtr);
        putLong(t.valuesAddr, idx, value);
        t.size++;
        if (intoEmptySlot) {
            t.used++;
        }
    }

    private void insertExisting(Table t, long keyPtr, int keyLen, int hash, long value) {
        int mask = t.capacity - 1;
        int idx = hash & mask;
        while (true) {
            byte state = getByte(t.statesAddr, idx);
            if (state == STATE_EMPTY) {
                putByte(t.statesAddr, idx, STATE_FILLED);
                putInt(t.hashesAddr, idx, hash);
                putInt(t.keyLenAddr, idx, keyLen);
                putLong(t.keyPtrAddr, idx, keyPtr);
                putLong(t.valuesAddr, idx, value);
                t.size++;
                t.used++;
                return;
            }
            idx = (idx + 1) & mask;
        }
    }

    private void freeKeyAt(Table t, int idx) {
        long keyPtr = getLong(t.keyPtrAddr, idx);
        int keyLen = getInt(t.keyLenAddr, idx);
        if (keyPtr != 0 && keyLen > 0) {
            allocator.freeAddress(keyPtr, keyLen);
        }
    }

    private int findIndex(Table t, byte[] key, int hash) {
        int mask = t.capacity - 1;
        int idx = hash & mask;
        while (true) {
            byte state = getByte(t.statesAddr, idx);
            if (state == STATE_EMPTY) {
                return -1;
            }
            if (state == STATE_FILLED) {
                if (getInt(t.hashesAddr, idx) == hash) {
                    int keyLen = getInt(t.keyLenAddr, idx);
                    if (keyLen == key.length) {
                        long keyPtr = getLong(t.keyPtrAddr, idx);
                        if (equalsKey(keyPtr, key, keyLen)) {
                            return idx;
                        }
                    }
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

    private int hash(byte[] key) {
        int h = java.util.Arrays.hashCode(key) ^ seed;
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

    private byte getByte(long base, int index) {
        return allocator.getByte(base + index);
    }

    private void putByte(long base, int index, byte value) {
        allocator.putByte(base + index, value);
    }

    private int getInt(long base, int index) {
        long addr = base + (long) index * INT_BYTES;
        int b0 = allocator.getByte(addr) & 0xff;
        int b1 = allocator.getByte(addr + 1) & 0xff;
        int b2 = allocator.getByte(addr + 2) & 0xff;
        int b3 = allocator.getByte(addr + 3) & 0xff;
        return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
    }

    private void putInt(long base, int index, int value) {
        long addr = base + (long) index * INT_BYTES;
        allocator.putByte(addr, (byte) value);
        allocator.putByte(addr + 1, (byte) (value >>> 8));
        allocator.putByte(addr + 2, (byte) (value >>> 16));
        allocator.putByte(addr + 3, (byte) (value >>> 24));
    }

    private long getLong(long base, int index) {
        long addr = base + (long) index * LONG_BYTES;
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
        long addr = base + (long) index * LONG_BYTES;
        allocator.putByte(addr, (byte) value);
        allocator.putByte(addr + 1, (byte) (value >>> 8));
        allocator.putByte(addr + 2, (byte) (value >>> 16));
        allocator.putByte(addr + 3, (byte) (value >>> 24));
        allocator.putByte(addr + 4, (byte) (value >>> 32));
        allocator.putByte(addr + 5, (byte) (value >>> 40));
        allocator.putByte(addr + 6, (byte) (value >>> 48));
        allocator.putByte(addr + 7, (byte) (value >>> 56));
    }

    public static final class KeyHandle {
        public final long keyPtr;
        public final int keyLen;
        public final int hash;

        private KeyHandle(long keyPtr, int keyLen, int hash) {
            this.keyPtr = keyPtr;
            this.keyLen = keyLen;
            this.hash = hash;
        }
    }

    public interface EntryConsumer {
        void accept(long keyPtr, int keyLen, long value);
    }

    private final class Table implements AutoCloseable {
        final int capacity;
        final int threshold;

        int size;
        int used;

        final OffHeapBlock statesBlock;
        final OffHeapBlock hashesBlock;
        final OffHeapBlock keyPtrBlock;
        final OffHeapBlock keyLenBlock;
        final OffHeapBlock valuesBlock;

        final long statesAddr;
        final long hashesAddr;
        final long keyPtrAddr;
        final long keyLenAddr;
        final long valuesAddr;

        private Table(int capacity) {
            if (Integer.bitCount(capacity) != 1) {
                throw new IllegalArgumentException("capacity must be power of two");
            }
            this.capacity = capacity;
            this.threshold = (int) Math.floor(capacity * LOAD_FACTOR);

            statesBlock = allocator.allocateBlock(capacity);
            hashesBlock = allocator.allocateBlock(capacity * INT_BYTES);
            keyPtrBlock = allocator.allocateBlock(capacity * LONG_BYTES);
            keyLenBlock = allocator.allocateBlock(capacity * INT_BYTES);
            valuesBlock = allocator.allocateBlock(capacity * LONG_BYTES);

            statesAddr = statesBlock.address();
            hashesAddr = hashesBlock.address();
            keyPtrAddr = keyPtrBlock.address();
            keyLenAddr = keyLenBlock.address();
            valuesAddr = valuesBlock.address();

            clearMemory(statesAddr, capacity);
            clearMemory(hashesAddr, capacity * INT_BYTES);
            clearMemory(keyPtrAddr, capacity * LONG_BYTES);
            clearMemory(keyLenAddr, capacity * INT_BYTES);
            clearMemory(valuesAddr, capacity * LONG_BYTES);
        }

        @Override
        public void close() {
            statesBlock.close();
            hashesBlock.close();
            keyPtrBlock.close();
            keyLenBlock.close();
            valuesBlock.close();
        }
    }

    private void clearMemory(long address, int bytes) {
        if (bytes <= 0) {
            return;
        }
        // Use a small heap chunk for zeroing to avoid depending on Unsafe.setMemory directly.
        byte[] zeros = new byte[Math.min(8 * 1024, bytes)];
        int remaining = bytes;
        long dst = address;
        while (remaining > 0) {
            int chunk = Math.min(remaining, zeros.length);
            allocator.copyMemory(zeros, 0, dst, chunk);
            dst += chunk;
            remaining -= chunk;
        }
    }
}
