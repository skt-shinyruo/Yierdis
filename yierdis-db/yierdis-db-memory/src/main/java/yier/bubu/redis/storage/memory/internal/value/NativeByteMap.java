package yier.bubu.redis.storage.memory.internal.value;

import java.util.Objects;
import java.util.function.ToIntFunction;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.memory.internal.hash.HashCapacityPolicy;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMetrics;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkBudget;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkResult;
import yier.bubu.redis.storage.memory.internal.hash.SipHash24;

public final class NativeByteMap<V> implements AutoCloseable {
    private static final byte STATE_EMPTY = 0;
    private static final byte STATE_FILLED = 1;
    private static final byte STATE_TOMBSTONE = 2;
    private static final byte STATE_MIGRATED_SCAN_SHADOW = 3;
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long REFERENCE_BYTES = 8L;
    private static final long TABLE_OBJECT_BYTES = 48L;
    private static final HashTableWorkBudget WRITE_REHASH_BUDGET = HashTableWorkBudget.of(2L, Long.MAX_VALUE);

    private final NativeByteStore byteStore;
    private final NativeObjectKind keyKind;
    private final HashSeed hashSeed;
    private final ToIntFunction<byte[]> hashOverride;
    private Table active;
    private Table old;
    private int rehashCursor;
    private int size;
    private long nativeBytes;
    private long generation;
    private long completedRehashes;
    private int maximumProbeLength;

    public NativeByteMap(NativeByteStore byteStore, NativeObjectKind keyKind) {
        this(byteStore, keyKind, HashSeed.random());
    }

    public NativeByteMap(NativeByteStore byteStore, NativeObjectKind keyKind, HashSeed hashSeed) {
        this(byteStore, keyKind, hashSeed, null);
    }

    NativeByteMap(
            NativeByteStore byteStore,
            NativeObjectKind keyKind,
            HashSeed hashSeed,
            ToIntFunction<byte[]> hashOverride
    ) {
        this.byteStore = Objects.requireNonNull(byteStore, "byteStore");
        this.keyKind = Objects.requireNonNull(keyKind, "keyKind");
        this.hashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
        this.hashOverride = hashOverride;
        this.active = new Table(HashCapacityPolicy.MIN_CAPACITY);
    }

    public int size() {
        return size;
    }

    public boolean containsKey(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        int hash = hash(keyBytes);
        return findIndex(active, keyBytes, hash) >= 0 || findIndex(old, keyBytes, hash) >= 0;
    }

    @SuppressWarnings("unchecked")
    public V get(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        int hash = hash(keyBytes);
        int index = findIndex(active, keyBytes, hash);
        if (index >= 0) {
            return (V) active.values[index];
        }
        index = findIndex(old, keyBytes, hash);
        return index < 0 ? null : (V) old.values[index];
    }

    @SuppressWarnings("unchecked")
    public V put(byte[] keyBytes, V value) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        advanceRehashOnWrite();
        int hash = hash(keyBytes);
        int index = findIndex(active, keyBytes, hash);
        if (index >= 0) {
            V previous = (V) active.values[index];
            active.values[index] = value;
            return previous;
        }
        index = findIndex(old, keyBytes, hash);
        if (index >= 0) {
            V previous = (V) old.values[index];
            moveOldSlotToActive(index);
            int activeIndex = findIndex(active, keyBytes, hash);
            if (activeIndex < 0) {
                throw new IllegalStateException("migrated native byte-map key is missing from active table");
            }
            active.values[activeIndex] = value;
            return previous;
        }

        Table staged = stageTableForInsert(hash);
        NativeHandle key = byteStore.store(keyBytes, keyKind);
        long keyBytesSize = byteStore.allocatedBytes(key);
        if (staged != null) {
            publishStagedTable(staged);
        }
        insertActive(key, value, hash);
        nativeBytes += keyBytesSize;
        return null;
    }

    @SuppressWarnings("unchecked")
    public V replace(byte[] keyBytes, V value) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        advanceRehashOnWrite();
        int hash = hash(keyBytes);
        int index = findIndex(active, keyBytes, hash);
        if (index >= 0) {
            V previous = (V) active.values[index];
            active.values[index] = value;
            return previous;
        }
        index = findIndex(old, keyBytes, hash);
        if (index < 0) {
            return null;
        }
        V previous = (V) old.values[index];
        moveOldSlotToActive(index);
        int activeIndex = findIndex(active, keyBytes, hash);
        if (activeIndex < 0) {
            throw new IllegalStateException("migrated native byte-map key is missing from active table");
        }
        active.values[activeIndex] = value;
        return previous;
    }

    @SuppressWarnings("unchecked")
    public V remove(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        advanceRehashOnWrite();
        int hash = hash(keyBytes);
        int index = findIndex(active, keyBytes, hash);
        if (index >= 0) {
            V previous = (V) active.values[index];
            removeFromActive(index);
            return previous;
        }
        index = findIndex(old, keyBytes, hash);
        if (index < 0) {
            return null;
        }
        V previous = (V) old.values[index];
        removeFromOld(index);
        return previous;
    }

    public void forEach(EntryConsumer<V> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        forEach(active, consumer);
        forEach(old, consumer);
    }

    public void clear() {
        releaseTableKeys(active);
        releaseTableKeys(old);
        active = new Table(HashCapacityPolicy.MIN_CAPACITY);
        old = null;
        rehashCursor = 0;
        size = 0;
        nativeBytes = 0L;
        generation++;
    }

    public long nativeBytes() {
        return nativeBytes;
    }

    public long heapBytes() {
        return active.heapBytes + (old == null ? 0L : old.heapBytes);
    }

    public long estimatedInsertHeapGrowthBytes() {
        if (old != null) {
            return 0L;
        }
        int projectedFilled = Math.min(active.capacity, active.filled + 1);
        int projectedSize = active.size + 1;
        int projectedTombstones = projectedFilled - projectedSize;
        if (projectedTombstones < 0) {
            return 0L;
        }
        HashCapacityPolicy.Decision decision = HashCapacityPolicy.nextAction(
                active.capacity,
                projectedSize,
                projectedFilled,
                projectedTombstones
        );
        return decision.action() == HashCapacityPolicy.Action.NONE
                ? 0L
                : heapBytesForCapacity(decision.targetCapacity());
    }

    public HashTableMetrics metrics() {
        return new HashTableMetrics(
                active.capacity,
                size,
                active.filled,
                active.tombstones,
                old != null,
                old == null ? 0 : old.capacity,
                old == null ? 0 : rehashCursor,
                generation,
                completedRehashes,
                maximumProbeLength
        );
    }

    public HashTableWorkResult advanceRehash(HashTableWorkBudget budget) {
        Objects.requireNonNull(budget, "budget");
        if (old == null) {
            return new HashTableWorkResult(0L, 0L, true, HashTableWorkResult.StopReason.NOT_REHASHING);
        }

        long inspected = 0L;
        long migrated = 0L;
        long startedAt = System.nanoTime();
        while (rehashCursor < old.capacity) {
            if (inspected >= budget.maxInspectedSlots()) {
                return new HashTableWorkResult(inspected, migrated, false, HashTableWorkResult.StopReason.SLOT_LIMIT);
            }
            if (timeLimitReached(startedAt, budget.timeLimitNanos())) {
                return new HashTableWorkResult(inspected, migrated, false, HashTableWorkResult.StopReason.TIME_LIMIT);
            }
            int index = rehashCursor++;
            inspected++;
            if (old.states[index] == STATE_FILLED) {
                moveOldSlotToActive(index);
                migrated++;
            }
        }

        old = null;
        rehashCursor = 0;
        completedRehashes++;
        generation++;
        return new HashTableWorkResult(inspected, migrated, true, HashTableWorkResult.StopReason.COMPLETE);
    }

    @Override
    public void close() {
        clear();
    }

    private void advanceRehashOnWrite() {
        if (old != null) {
            advanceRehash(WRITE_REHASH_BUDGET);
        }
    }

    private void forEach(Table table, EntryConsumer<V> consumer) {
        if (table == null) {
            return;
        }
        for (int i = 0; i < table.capacity; i++) {
            if (table.states[i] == STATE_FILLED) {
                @SuppressWarnings("unchecked")
                V value = (V) table.values[i];
                consumer.accept(table.keys[i], value);
            }
        }
    }

    private void releaseTableKeys(Table table) {
        if (table == null) {
            return;
        }
        for (int i = 0; i < table.capacity; i++) {
            if (table.states[i] == STATE_FILLED) {
                byteStore.release(table.keys[i]);
                table.keys[i] = null;
                table.values[i] = null;
                table.hashes[i] = 0;
                table.states[i] = STATE_EMPTY;
            }
        }
    }

    private Table stageTableForInsert(int hash) {
        if (old != null) {
            return null;
        }
        int insertionIndex = findInsertionIndex(active, hash);
        byte previousState = active.states[insertionIndex];
        int projectedSize = active.size + 1;
        int projectedFilled = active.filled + (previousState == STATE_EMPTY ? 1 : 0);
        int projectedTombstones = active.tombstones - (previousState == STATE_TOMBSTONE ? 1 : 0);
        HashCapacityPolicy.Decision decision = HashCapacityPolicy.nextAction(
                active.capacity,
                projectedSize,
                projectedFilled,
                projectedTombstones
        );
        return decision.action() == HashCapacityPolicy.Action.NONE ? null : new Table(decision.targetCapacity());
    }

    private void publishStagedTable(Table staged) {
        if (old != null) {
            throw new IllegalStateException("cannot start a second native byte-map rehash");
        }
        old = active;
        active = Objects.requireNonNull(staged, "staged");
        rehashCursor = 0;
        generation++;
    }

    private void insertActive(NativeHandle key, V value, int hash) {
        int index = findInsertionIndex(active, hash);
        if (active.states[index] == STATE_FILLED) {
            throw new IllegalStateException("native byte-map key already exists during insert");
        }
        writeFilled(active, index, key, value, hash);
        size++;
    }

    private void moveOldSlotToActive(int oldIndex) {
        if (old == null || old.states[oldIndex] != STATE_FILLED) {
            return;
        }
        NativeHandle key = old.keys[oldIndex];
        Object value = old.values[oldIndex];
        int hash = old.hashes[oldIndex];
        int activeIndex = findInsertionIndex(active, hash);
        if (active.states[activeIndex] == STATE_FILLED) {
            throw new IllegalStateException("duplicate native byte-map key while rehashing");
        }
        writeFilled(active, activeIndex, key, value, hash);
        old.states[oldIndex] = STATE_MIGRATED_SCAN_SHADOW;
        old.size--;
        old.filled--;
    }

    private void writeFilled(Table table, int index, NativeHandle key, Object value, int hash) {
        byte previous = table.states[index];
        if (previous == STATE_EMPTY) {
            table.filled++;
        } else if (previous == STATE_TOMBSTONE) {
            table.tombstones--;
        } else {
            throw new IllegalStateException("attempted to overwrite a live native byte-map slot");
        }
        table.keys[index] = Objects.requireNonNull(key, "key");
        table.values[index] = value;
        table.hashes[index] = hash;
        table.states[index] = STATE_FILLED;
        table.size++;
    }

    private void removeFromActive(int index) {
        NativeHandle key = active.keys[index];
        int hash = active.hashes[index];
        invalidateOldShadow(key, hash);
        removeFromTable(active, index);
    }

    private void removeFromOld(int index) {
        removeFromTable(old, index);
    }

    private void removeFromTable(Table table, int index) {
        if (table == null || table.states[index] != STATE_FILLED) {
            return;
        }
        NativeHandle key = table.keys[index];
        nativeBytes -= byteStore.allocatedBytes(key);
        byteStore.release(key);
        table.keys[index] = null;
        table.values[index] = null;
        table.hashes[index] = 0;
        table.states[index] = STATE_TOMBSTONE;
        table.size--;
        table.tombstones++;
        size--;
    }

    private void invalidateOldShadow(NativeHandle key, int hash) {
        if (old == null) {
            return;
        }
        int mask = old.capacity - 1;
        int index = hash & mask;
        for (int probes = 0; probes < old.capacity; probes++) {
            byte state = old.states[index];
            if (state == STATE_EMPTY) {
                return;
            }
            if (state == STATE_MIGRATED_SCAN_SHADOW
                    && old.hashes[index] == hash
                    && key.equals(old.keys[index])) {
                old.keys[index] = null;
                old.values[index] = null;
                old.hashes[index] = 0;
                old.states[index] = STATE_TOMBSTONE;
                old.filled++;
                old.tombstones++;
                return;
            }
            index = (index + 1) & mask;
        }
    }

    private int findIndex(Table table, byte[] keyBytes, int hash) {
        if (table == null) {
            return -1;
        }
        int mask = table.capacity - 1;
        int index = hash & mask;
        for (int probes = 1; probes <= table.capacity; probes++) {
            recordProbe(probes);
            byte state = table.states[index];
            if (state == STATE_EMPTY) {
                return -1;
            }
            if (state == STATE_FILLED
                    && table.hashes[index] == hash
                    && byteStore.equalsBytes(table.keys[index], keyBytes)) {
                return index;
            }
            index = (index + 1) & mask;
        }
        return -1;
    }

    private int findInsertionIndex(Table table, int hash) {
        int mask = table.capacity - 1;
        int index = hash & mask;
        int firstTombstone = -1;
        for (int probes = 1; probes <= table.capacity; probes++) {
            recordProbe(probes);
            byte state = table.states[index];
            if (state == STATE_EMPTY) {
                return firstTombstone >= 0 ? firstTombstone : index;
            }
            if (state == STATE_TOMBSTONE && firstTombstone < 0) {
                firstTombstone = index;
            }
            index = (index + 1) & mask;
        }
        if (firstTombstone >= 0) {
            return firstTombstone;
        }
        throw new IllegalStateException("native byte-map has no insertion slot");
    }

    private void recordProbe(int probes) {
        if (probes > maximumProbeLength) {
            maximumProbeLength = probes;
        }
    }

    private int hash(byte[] keyBytes) {
        return hashOverride == null
                ? SipHash24.foldToInt(SipHash24.hash(hashSeed, keyBytes))
                : hashOverride.applyAsInt(keyBytes);
    }

    private static boolean timeLimitReached(long startedAt, long timeLimitNanos) {
        return timeLimitNanos != Long.MAX_VALUE && System.nanoTime() - startedAt >= timeLimitNanos;
    }

    private static long heapBytesForCapacity(int capacity) {
        long keyArray = ARRAY_HEADER_BYTES + (long) capacity * REFERENCE_BYTES;
        long valueArray = ARRAY_HEADER_BYTES + (long) capacity * REFERENCE_BYTES;
        long hashArray = ARRAY_HEADER_BYTES + (long) capacity * Integer.BYTES;
        long stateArray = ARRAY_HEADER_BYTES + capacity;
        return TABLE_OBJECT_BYTES + keyArray + valueArray + hashArray + stateArray;
    }

    @FunctionalInterface
    public interface EntryConsumer<V> {
        void accept(NativeHandle keyHandle, V value);
    }

    private static final class Table {
        private final int capacity;
        private final byte[] states;
        private final int[] hashes;
        private final NativeHandle[] keys;
        private final Object[] values;
        private final long heapBytes;
        private int size;
        private int filled;
        private int tombstones;

        private Table(int capacity) {
            if (capacity < HashCapacityPolicy.MIN_CAPACITY
                    || capacity > HashCapacityPolicy.MAX_CAPACITY
                    || (capacity & (capacity - 1)) != 0) {
                throw new IllegalArgumentException("invalid native byte-map capacity: " + capacity);
            }
            this.capacity = capacity;
            this.states = new byte[capacity];
            this.hashes = new int[capacity];
            this.keys = new NativeHandle[capacity];
            this.values = new Object[capacity];
            this.heapBytes = heapBytesForCapacity(capacity);
        }
    }
}
