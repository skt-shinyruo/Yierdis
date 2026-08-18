package yier.bubu.redis.storage.memory.internal.value;

import static yier.bubu.redis.common.memory.MemoryUsageSnapshot.addSaturating;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.ToIntFunction;
import yier.bubu.redis.memory.api.NativeCapacityExceededException;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.memory.internal.hash.HashCapacityPolicy;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMetrics;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkBudget;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkResult;
import yier.bubu.redis.storage.memory.internal.hash.SipHash24;

public final class NativeByteMap<V> implements AutoCloseable, HashTableMaintenanceRegistry.Participant {
    private static final byte STATE_EMPTY = 0;
    private static final byte STATE_FILLED = 1;
    private static final byte STATE_TOMBSTONE = 2;
    private static final byte STATE_MIGRATED_SCAN_SHADOW = 3;
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long REFERENCE_BYTES = 8L;
    private static final long TABLE_OBJECT_BYTES = 48L;
    private static final long PREPARED_MUTATION_OBJECT_BYTES = 128L;
    private static final HashTableWorkBudget WRITE_REHASH_BUDGET = HashTableWorkBudget.of(2L, Long.MAX_VALUE);

    private final NativeByteStore byteStore;
    private final NativeObjectKind keyKind;
    private final HashSeed hashSeed;
    private final ToIntFunction<byte[]> hashOverride;
    private final HashTableMaintenanceRegistry maintenanceRegistry;
    private final HashTableMaintenanceRegistry.Registration maintenanceRegistration;
    private final Runnable heapChangeListener;
    private final ValueLayout valueLayout;
    private final V constantValue;
    private final boolean ownsKeys;
    private Table active;
    private Table old;
    private int rehashCursor;
    private int size;
    private long nativeBytes;
    private long generation;
    private long contentGeneration;
    private long completedRehashes;
    private int maximumProbeLength;
    private boolean maintenanceDebt;

    public NativeByteMap(NativeByteStore byteStore, NativeObjectKind keyKind) {
        this(byteStore, keyKind, HashSeed.random());
    }

    public NativeByteMap(NativeByteStore byteStore, NativeObjectKind keyKind, HashSeed hashSeed) {
        this(byteStore, keyKind, hashSeed, null, null, null);
    }

    public NativeByteMap(
            NativeByteStore byteStore,
            NativeObjectKind keyKind,
            HashSeed hashSeed,
            HashTableMaintenanceRegistry maintenanceRegistry
    ) {
        this(byteStore, keyKind, hashSeed, maintenanceRegistry, null);
    }

    public NativeByteMap(
            NativeByteStore byteStore,
            NativeObjectKind keyKind,
            HashSeed hashSeed,
            HashTableMaintenanceRegistry maintenanceRegistry,
            Runnable heapChangeListener
    ) {
        this(byteStore, keyKind, hashSeed, null, maintenanceRegistry, heapChangeListener);
    }

    NativeByteMap(
            NativeByteStore byteStore,
            NativeObjectKind keyKind,
            HashSeed hashSeed,
            ToIntFunction<byte[]> hashOverride
    ) {
        this(byteStore, keyKind, hashSeed, hashOverride, null, null);
    }

    NativeByteMap(
            NativeByteStore byteStore,
            NativeObjectKind keyKind,
            HashSeed hashSeed,
            ToIntFunction<byte[]> hashOverride,
            HashTableMaintenanceRegistry maintenanceRegistry
    ) {
        this(byteStore, keyKind, hashSeed, hashOverride, maintenanceRegistry, null);
    }

    NativeByteMap(
            NativeByteStore byteStore,
            NativeObjectKind keyKind,
            HashSeed hashSeed,
            ToIntFunction<byte[]> hashOverride,
            HashTableMaintenanceRegistry maintenanceRegistry,
            Runnable heapChangeListener
    ) {
        this(
                byteStore,
                keyKind,
                hashSeed,
                hashOverride,
                maintenanceRegistry,
                heapChangeListener,
                ValueLayout.OBJECT_REFERENCES,
                null,
                true
        );
    }

    static NativeByteMap<NativeHandle> nativeHandleValues(
            NativeByteStore byteStore,
            NativeObjectKind keyKind,
            HashSeed hashSeed,
            HashTableMaintenanceRegistry maintenanceRegistry,
            Runnable heapChangeListener
    ) {
        return new NativeByteMap<>(
                byteStore,
                keyKind,
                hashSeed,
                null,
                maintenanceRegistry,
                heapChangeListener,
                ValueLayout.NATIVE_HANDLES,
                null,
                true
        );
    }

    static NativeByteMap<NativeHandle> nativeHandleValues(
            NativeByteStore byteStore,
            NativeObjectKind keyKind,
            HashSeed hashSeed,
            ToIntFunction<byte[]> hashOverride
    ) {
        return new NativeByteMap<>(
                byteStore,
                keyKind,
                hashSeed,
                hashOverride,
                null,
                null,
                ValueLayout.NATIVE_HANDLES,
                null,
                true
        );
    }

    static <V> NativeByteMap<V> constantValues(
            NativeByteStore byteStore,
            NativeObjectKind keyKind,
            HashSeed hashSeed,
            HashTableMaintenanceRegistry maintenanceRegistry,
            Runnable heapChangeListener,
            V constantValue
    ) {
        return new NativeByteMap<>(
                byteStore,
                keyKind,
                hashSeed,
                null,
                maintenanceRegistry,
                heapChangeListener,
                ValueLayout.CONSTANT,
                Objects.requireNonNull(constantValue, "constantValue"),
                true
        );
    }

    static <V> NativeByteMap<V> borrowedKeys(
            NativeByteStore byteStore,
            NativeObjectKind keyKind,
            HashSeed hashSeed,
            HashTableMaintenanceRegistry maintenanceRegistry,
            Runnable heapChangeListener
    ) {
        return new NativeByteMap<>(
                byteStore,
                keyKind,
                hashSeed,
                null,
                maintenanceRegistry,
                heapChangeListener,
                ValueLayout.OBJECT_REFERENCES,
                null,
                false
        );
    }

    private NativeByteMap(
            NativeByteStore byteStore,
            NativeObjectKind keyKind,
            HashSeed hashSeed,
            ToIntFunction<byte[]> hashOverride,
            HashTableMaintenanceRegistry maintenanceRegistry,
            Runnable heapChangeListener,
            ValueLayout valueLayout,
            V constantValue,
            boolean ownsKeys
    ) {
        this.byteStore = Objects.requireNonNull(byteStore, "byteStore");
        this.keyKind = Objects.requireNonNull(keyKind, "keyKind");
        this.hashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
        this.hashOverride = hashOverride;
        this.maintenanceRegistry = maintenanceRegistry;
        this.maintenanceRegistration = maintenanceRegistry == null ? null : maintenanceRegistry.registration(this);
        this.heapChangeListener = heapChangeListener;
        this.valueLayout = Objects.requireNonNull(valueLayout, "valueLayout");
        this.constantValue = constantValue;
        this.ownsKeys = ownsKeys;
        this.active = new Table(HashCapacityPolicy.MIN_CAPACITY, valueLayout);
    }

    public int size() {
        return size;
    }

    public boolean containsKey(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        int hash = hash(keyBytes);
        return findIndex(active, keyBytes, hash) >= 0 || findIndex(old, keyBytes, hash) >= 0;
    }

    public V get(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        int hash = hash(keyBytes);
        int index = findIndex(active, keyBytes, hash);
        if (index >= 0) {
            return valueAt(active, index);
        }
        index = findIndex(old, keyBytes, hash);
        return index < 0 ? null : valueAt(old, index);
    }

    public V put(byte[] keyBytes, V value) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        requireOwnedKeys();
        advanceRehashOnWrite();
        int hash = hash(keyBytes);
        int index = findIndex(active, keyBytes, hash);
        if (index >= 0) {
            V previous = valueAt(active, index);
            writeValue(active, index, value);
            contentGeneration++;
            return previous;
        }
        index = findIndex(old, keyBytes, hash);
        if (index >= 0) {
            V previous = valueAt(old, index);
            moveOldSlotToActive(index);
            int activeIndex = findIndex(active, keyBytes, hash);
            if (activeIndex < 0) {
                throw new IllegalStateException("migrated native byte-map key is missing from active table");
            }
            writeValue(active, activeIndex, value);
            contentGeneration++;
            return previous;
        }

        Table staged = stageTableForInsert(hash);
        NativeHandle keyHandle = byteStore.store(keyBytes, keyKind);
        long keyBytesSize = byteStore.allocatedBytes(keyHandle);
        if (staged != null) {
            publishStagedTable(staged);
        }
        insertActive(keyHandle, value, hash);
        nativeBytes += keyBytesSize;
        contentGeneration++;
        return null;
    }

    V putBorrowed(NativeHandle keyHandle, V value) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        if (ownsKeys) {
            throw new IllegalStateException("borrowed-key insertion requires a borrowed-key map");
        }
        validateValue(value);
        advanceRehashOnWrite();
        if (keyHandle.isNull()) {
            throw new IllegalArgumentException("keyHandle must not be null");
        }
        int hash = hashHandle(keyHandle);
        int index = findIndex(active, keyHandle, hash);
        if (index >= 0) {
            V previous = valueAt(active, index);
            writeValue(active, index, value);
            contentGeneration++;
            return previous;
        }
        index = findIndex(old, keyHandle, hash);
        if (index >= 0) {
            V previous = valueAt(old, index);
            moveOldSlotToActive(index);
            int activeIndex = findIndex(active, keyHandle, hash);
            if (activeIndex < 0) {
                throw new IllegalStateException("migrated native byte-map key is missing from active table");
            }
            writeValue(active, activeIndex, value);
            contentGeneration++;
            return previous;
        }

        Table staged = stageTableForInsert(hash);
        if (staged != null) {
            publishStagedTable(staged);
        }
        insertActive(keyHandle, value, hash);
        contentGeneration++;
        return null;
    }

    PreparedMutation<V> preparePuts(List<StagedPut<V>> puts) {
        Objects.requireNonNull(puts, "puts");
        @SuppressWarnings("unchecked")
        StagedPut<V>[] stagedPuts = puts.toArray(StagedPut[]::new);
        for (StagedPut<V> put : stagedPuts) {
            Objects.requireNonNull(put, "put");
            validateValue(put.nextValue);
            if (ownsKeys && put.borrowedKeyHandle != null) {
                throw new IllegalArgumentException("owning native byte-map cannot stage a borrowed key");
            }
            if (!ownsKeys) {
                if (put.borrowedKeyHandle == null || put.borrowedKeyHandle.isNull()) {
                    throw new IllegalArgumentException("borrowed native byte-map requires a borrowed key handle");
                }
                if (!byteStore.equalsBytes(put.borrowedKeyHandle, put.keyBytes)) {
                    throw new IllegalArgumentException("borrowed native byte-map key bytes do not match the handle");
                }
            }
        }
        return new PreparedMutation<>(this, stagedPuts);
    }

    long estimatedPreparedPutHeapGrowthBytes(int putCount, int addedCount) {
        if (putCount < 0 || addedCount < 0 || addedCount > putCount) {
            throw new IllegalArgumentException("invalid prepared native byte-map put counts");
        }
        int topologyCapacity = replacementTopologyCapacity(addedCount);
        return preparedMutationHeapBytes(putCount, topologyCapacity, valueLayout);
    }

    public V replace(byte[] keyBytes, V value) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        advanceRehashOnWrite();
        int hash = hash(keyBytes);
        int index = findIndex(active, keyBytes, hash);
        if (index >= 0) {
            V previous = valueAt(active, index);
            writeValue(active, index, value);
            contentGeneration++;
            return previous;
        }
        index = findIndex(old, keyBytes, hash);
        if (index < 0) {
            return null;
        }
        V previous = valueAt(old, index);
        moveOldSlotToActive(index);
        int activeIndex = findIndex(active, keyBytes, hash);
        if (activeIndex < 0) {
            throw new IllegalStateException("migrated native byte-map key is missing from active table");
        }
        writeValue(active, activeIndex, value);
        contentGeneration++;
        return previous;
    }

    public V remove(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        advanceRehashOnWrite();
        int hash = hash(keyBytes);
        int index = findIndex(active, keyBytes, hash);
        if (index >= 0) {
            V previous = valueAt(active, index);
            removeFromActive(index);
            contentGeneration++;
            return previous;
        }
        index = findIndex(old, keyBytes, hash);
        if (index < 0) {
            return null;
        }
        V previous = valueAt(old, index);
        removeFromOld(index);
        contentGeneration++;
        return previous;
    }

    public V remove(NativeHandle keyHandle) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        advanceRehashOnWrite();
        int index = findIndex(active, keyHandle);
        if (index >= 0) {
            V previous = valueAt(active, index);
            removeFromActive(index);
            contentGeneration++;
            return previous;
        }
        index = findIndex(old, keyHandle);
        if (index < 0) {
            return null;
        }
        V previous = valueAt(old, index);
        removeFromOld(index);
        contentGeneration++;
        return previous;
    }

    public void forEach(EntryConsumer<V> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        forEach(active, consumer);
        forEach(old, consumer);
    }

    public ScanCursorV2 scan(ScanCursorV2 cursor, int maxSteps, ScanConsumer<V> consumer) {
        return scanWithWork(cursor, Math.max(0L, maxSteps), consumer).nextCursor();
    }

    /**
     * 按底层槽位推进一次 Redis SCAN 风格遍历。返回结果允许重复元素，但 active/old 双表迁移期间不会
     * 因已迁移槽位从 old 表消失而漏掉持续存在的元素。
     */
    public ScanResult scanWithWork(ScanCursorV2 cursor, long maxSteps, ScanConsumer<V> consumer) {
        Objects.requireNonNull(cursor, "cursor");
        Objects.requireNonNull(consumer, "consumer");
        if (maxSteps < 0L) {
            throw new IllegalArgumentException("maxSteps must be >= 0");
        }

        ScanCursorV2 start = normalizeScanCursor(cursor);
        if (size == 0) {
            return new ScanResult(start, ScanCursorV2.start(), 0L, generation);
        }

        int phase = start.phase();
        long position = start.position();
        long inspected = 0L;
        while (true) {
            Table table = tableForPhase(phase);
            if (table == null || position >= table.capacity) {
                if (phase == 0 && old != null) {
                    phase = 1;
                    position = 0L;
                    continue;
                }
                return new ScanResult(start, ScanCursorV2.start(), inspected, generation);
            }

            if (inspected >= maxSteps) {
                return new ScanResult(start, cursorFor(phase, position), inspected, generation);
            }

            int index = (int) position;
            position++;
            inspected++;
            if (!acceptScanSlot(table, phase, index, consumer)) {
                if (position >= table.capacity) {
                    if (phase == 0 && old != null) {
                        return new ScanResult(start, cursorFor(1, 0L), inspected, generation);
                    }
                    return new ScanResult(start, ScanCursorV2.start(), inspected, generation);
                }
                return new ScanResult(start, cursorFor(phase, position), inspected, generation);
            }
        }
    }

    public void clear() {
        releaseTableKeys(active);
        releaseTableKeys(old);
        active = new Table(HashCapacityPolicy.MIN_CAPACITY, valueLayout);
        old = null;
        rehashCursor = 0;
        size = 0;
        nativeBytes = 0L;
        maintenanceDebt = false;
        refreshMaintenanceRegistration();
        generation++;
        contentGeneration++;
        notifyHeapChanged();
    }

    public long nativeBytes() {
        return nativeBytes;
    }

    public long heapBytes() {
        return active.heapBytes + (old == null ? 0L : old.heapBytes);
    }

    public long heapEstimatedBytes() {
        return heapBytes();
    }

    static long heapUpperBoundForEntries(long expectedEntries) {
        return heapUpperBoundForEntries(expectedEntries, ValueLayout.OBJECT_REFERENCES);
    }

    private static long heapUpperBoundForEntries(long expectedEntries, ValueLayout valueLayout) {
        if (expectedEntries < 0L || expectedEntries > HashCapacityPolicy.MAX_CAPACITY / 2L) {
            return Long.MAX_VALUE;
        }
        long requiredCapacity = Math.max(HashCapacityPolicy.MIN_CAPACITY, expectedEntries * 2L);
        int capacity = HashCapacityPolicy.MIN_CAPACITY;
        while (capacity < requiredCapacity) {
            capacity <<= 1;
        }
        long tableBytes = heapBytesForCapacity(capacity, valueLayout);
        return tableBytes > Long.MAX_VALUE - tableBytes ? Long.MAX_VALUE : tableBytes + tableBytes;
    }

    static long heapUpperBoundForNativeHandleValues(long expectedEntries) {
        return heapUpperBoundForEntries(expectedEntries, ValueLayout.NATIVE_HANDLES);
    }

    static long heapUpperBoundForConstantValues(long expectedEntries) {
        return heapUpperBoundForEntries(expectedEntries, ValueLayout.CONSTANT);
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
                : heapBytesForCapacity(decision.targetCapacity(), valueLayout);
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

    @Override
    public boolean hasMaintenanceDebt() {
        return maintenanceDebt || old != null;
    }

    @Override
    public long estimatedMaintenanceGrowthBytes() {
        if (old != null) {
            return 0L;
        }
        HashCapacityPolicy.Decision decision = maintenanceDecision();
        return decision.action() == HashCapacityPolicy.Action.NONE
                ? 0L
                : heapBytesForCapacity(decision.targetCapacity(), valueLayout);
    }

    @Override
    public HashTableMaintenanceRegistry.MaintenancePreparation prepareMaintenance() {
        StagedResize staged = stageMaintenanceResize();
        if (staged == null) {
            return null;
        }
        return new HashTableMaintenanceRegistry.MaintenancePreparation() {
            private StagedResize pending = staged;

            @Override
            public long stagedNonNativeGrowthBytes() {
                return requirePending().stagedHeapBytes();
            }

            @Override
            public void commit() {
                StagedResize current = requirePending();
                pending = null;
                publishStagedResize(current);
            }

            @Override
            public void abort() {
                StagedResize current = pending;
                pending = null;
                if (current != null) {
                    current.close();
                }
            }

            private StagedResize requirePending() {
                if (pending == null) {
                    throw new IllegalStateException("staged native byte-map maintenance resize is closed");
                }
                return pending;
            }
        };
    }

    public StagedResize stageMaintenanceResize() {
        if (old != null) {
            return null;
        }
        HashCapacityPolicy.Decision decision = maintenanceDecision();
        if (decision.action() == HashCapacityPolicy.Action.NONE) {
            maintenanceDebt = false;
            refreshMaintenanceRegistration();
            return null;
        }
        return new StagedResize(active, new Table(decision.targetCapacity(), valueLayout));
    }

    public void publishStagedResize(StagedResize staged) {
        Objects.requireNonNull(staged, "staged");
        staged.ensureActive();
        if (old != null || active != staged.source) {
            throw new IllegalStateException("staged native byte-map resize is no longer current");
        }
        publishStagedTable(staged.publish());
    }

    @Override
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
        recordMaintenanceDebt();
        notifyHeapChanged();
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
                consumer.accept(keyHandleAt(table, i), valueAt(table, i));
            }
        }
    }

    private boolean acceptScanSlot(Table table, int phase, int index, ScanConsumer<V> consumer) {
        byte state = table.states[index];
        if (state == STATE_FILLED) {
            return consumer.accept(keyHandleAt(table, index), valueAt(table, index));
        }
        if (phase != 1 || state != STATE_MIGRATED_SCAN_SHADOW) {
            return true;
        }

        // shadow 只保留迁移定位信息；active 中的 value 可能已替换，旧 value 甚至已由上层释放。
        int activeIndex = findStoredKeyIndex(active, table.keyHandles[index], table.hashes[index]);
        return activeIndex < 0
                || consumer.accept(keyHandleAt(active, activeIndex), valueAt(active, activeIndex));
    }

    private ScanCursorV2 normalizeScanCursor(ScanCursorV2 cursor) {
        int currentGeneration = wireGeneration();
        // generation 不匹配时旧表布局已不再可信，只能从当前 active 表重新开始并接受可能的重复。
        if (cursor.value() == 0L || cursor.generation() != currentGeneration) {
            return ScanCursorV2.of(currentGeneration, 0, 0L);
        }
        if (cursor.phase() == 1 && old == null) {
            return ScanCursorV2.of(currentGeneration, 0, 0L);
        }
        return cursor;
    }

    private Table tableForPhase(int phase) {
        return phase == 0 ? active : old;
    }

    private ScanCursorV2 cursorFor(int phase, long position) {
        return ScanCursorV2.of(wireGeneration(), phase, position);
    }

    private int wireGeneration() {
        return (int) (generation & 0x1fff_ffffL);
    }

    private void releaseTableKeys(Table table) {
        if (table == null) {
            return;
        }
        for (int i = 0; i < table.capacity; i++) {
            if (table.states[i] == STATE_FILLED) {
                if (ownsKeys) {
                    byteStore.release(table.keyHandles[i]);
                }
                table.keyHandles[i] = null;
                clearValue(table, i);
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
        return decision.action() == HashCapacityPolicy.Action.NONE
                ? null
                : new Table(decision.targetCapacity(), valueLayout);
    }

    private void publishStagedTable(Table staged) {
        if (old != null) {
            throw new IllegalStateException("cannot start a second native byte-map rehash");
        }
        old = active;
        active = Objects.requireNonNull(staged, "staged");
        rehashCursor = 0;
        maintenanceDebt = true;
        refreshMaintenanceRegistration();
        generation++;
        notifyHeapChanged();
    }

    private void insertActive(NativeHandle keyHandle, V value, int hash) {
        int index = findInsertionIndex(active, hash);
        if (active.states[index] == STATE_FILLED) {
            throw new IllegalStateException("native byte-map key already exists during insert");
        }
        writeFilled(active, index, keyHandle, value, hash);
        size++;
    }

    private void moveOldSlotToActive(int oldIndex) {
        if (old == null || old.states[oldIndex] != STATE_FILLED) {
            return;
        }
        NativeHandle keyHandle = old.keyHandles[oldIndex];
        int hash = old.hashes[oldIndex];
        int activeIndex = findInsertionIndex(active, hash);
        if (active.states[activeIndex] == STATE_FILLED) {
            throw new IllegalStateException("duplicate native byte-map key while rehashing");
        }
        writeMovedSlot(old, oldIndex, active, activeIndex, keyHandle, hash);
        old.states[oldIndex] = STATE_MIGRATED_SCAN_SHADOW;
        old.size--;
        old.filled--;
    }

    private void writeFilled(Table table, int index, NativeHandle keyHandle, V value, int hash) {
        byte previous = requireWritableState(table, index);
        writeValue(table, index, value);
        writeSlotMetadata(table, index, keyHandle, hash, previous);
    }

    private void writeMovedSlot(
            Table source,
            int sourceIndex,
            Table target,
            int targetIndex,
            NativeHandle keyHandle,
            int hash
    ) {
        byte previous = requireWritableState(target, targetIndex);
        moveValue(source, sourceIndex, target, targetIndex);
        writeSlotMetadata(target, targetIndex, keyHandle, hash, previous);
    }

    private static byte requireWritableState(Table table, int index) {
        byte previous = table.states[index];
        if (previous != STATE_EMPTY && previous != STATE_TOMBSTONE) {
            throw new IllegalStateException("attempted to overwrite a live native byte-map slot");
        }
        return previous;
    }

    private static void writeSlotMetadata(
            Table table,
            int index,
            NativeHandle keyHandle,
            int hash,
            byte previous
    ) {
        if (previous == STATE_EMPTY) {
            table.filled++;
        } else {
            table.tombstones--;
        }
        table.keyHandles[index] = keyHandle;
        table.hashes[index] = hash;
        table.states[index] = STATE_FILLED;
        table.size++;
    }

    private void removeFromActive(int index) {
        NativeHandle keyHandle = active.keyHandles[index];
        int hash = active.hashes[index];
        invalidateOldShadow(keyHandle, hash);
        removeFromTable(active, index);
    }

    private void removeFromOld(int index) {
        removeFromTable(old, index);
    }

    private void removeFromTable(Table table, int index) {
        if (table == null || table.states[index] != STATE_FILLED) {
            return;
        }
        NativeHandle keyHandle = table.keyHandles[index];
        if (ownsKeys) {
            nativeBytes -= byteStore.allocatedBytes(keyHandle);
            byteStore.release(keyHandle);
        }
        table.keyHandles[index] = null;
        clearValue(table, index);
        table.hashes[index] = 0;
        table.states[index] = STATE_TOMBSTONE;
        table.size--;
        table.tombstones++;
        size--;
        recordMaintenanceDebt();
    }

    private HashCapacityPolicy.Decision maintenanceDecision() {
        return HashCapacityPolicy.nextAction(
                active.capacity,
                active.size,
                active.filled,
                active.tombstones
        );
    }

    private void recordMaintenanceDebt() {
        if (old != null) {
            maintenanceDebt = true;
            refreshMaintenanceRegistration();
            return;
        }
        maintenanceDebt = maintenanceDecision().action() != HashCapacityPolicy.Action.NONE;
        refreshMaintenanceRegistration();
    }

    private void refreshMaintenanceRegistration() {
        if (maintenanceRegistration == null) {
            return;
        }
        if (maintenanceDebt || old != null) {
            if (!maintenanceRegistration.registered()) {
                maintenanceRegistry.register(maintenanceRegistration);
            }
        } else {
            maintenanceRegistry.unregister(maintenanceRegistration);
        }
    }

    private void notifyHeapChanged() {
        if (heapChangeListener != null) {
            heapChangeListener.run();
        }
    }

    private void invalidateOldShadow(NativeHandle keyHandle, int hash) {
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
                    && keyHandle.equals(old.keyHandles[index])) {
                old.keyHandles[index] = null;
                clearValue(old, index);
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
                    && byteStore.equalsBytes(table.keyHandles[index], keyBytes)) {
                return index;
            }
            index = (index + 1) & mask;
        }
        return -1;
    }

    private int findIndex(Table table, NativeHandle keyHandle) {
        if (table == null) {
            return -1;
        }
        for (int index = 0; index < table.capacity; index++) {
            if (table.states[index] == STATE_FILLED
                    && byteStore.compareLex(table.keyHandles[index], keyHandle) == 0) {
                return index;
            }
        }
        return -1;
    }

    private int findIndex(Table table, NativeHandle keyHandle, int hash) {
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
                    && byteStore.compareLex(table.keyHandles[index], keyHandle) == 0) {
                return index;
            }
            index = (index + 1) & mask;
        }
        return -1;
    }

    private int findStoredKeyIndex(Table table, NativeHandle keyHandle, int hash) {
        if (table == null || keyHandle == null || keyHandle.isNull()) {
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
                    && keyHandle.equals(table.keyHandles[index])) {
                return index;
            }
            index = (index + 1) & mask;
        }
        return -1;
    }

    private static NativeHandle keyHandleAt(Table table, int index) {
        return table.keyHandles[index];
    }

    @SuppressWarnings("unchecked")
    private V valueAt(Table table, int index) {
        return switch (valueLayout) {
            case OBJECT_REFERENCES -> (V) ((Object[]) table.valueSlots)[index];
            case NATIVE_HANDLES -> (V) ((NativeHandle[]) table.valueSlots)[index];
            case CONSTANT -> constantValue;
        };
    }

    private void writeValue(Table table, int index, V value) {
        validateValue(value);
        switch (valueLayout) {
            case OBJECT_REFERENCES -> ((Object[]) table.valueSlots)[index] = value;
            case NATIVE_HANDLES -> ((NativeHandle[]) table.valueSlots)[index] = (NativeHandle) value;
            case CONSTANT -> {
            }
        }
    }

    private void validateValue(V value) {
        if (valueLayout == ValueLayout.NATIVE_HANDLES && value != null) {
            if (!(value instanceof NativeHandle handle) || handle.isNull()) {
                throw new IllegalArgumentException("native-handle value layout requires live NativeHandle values");
            }
        }
        if (valueLayout == ValueLayout.CONSTANT && value != constantValue) {
            throw new IllegalArgumentException("constant value layout requires the configured value");
        }
    }

    private void moveValue(Table source, int sourceIndex, Table target, int targetIndex) {
        switch (valueLayout) {
            case OBJECT_REFERENCES -> {
                Object[] sourceValues = (Object[]) source.valueSlots;
                ((Object[]) target.valueSlots)[targetIndex] = sourceValues[sourceIndex];
                sourceValues[sourceIndex] = null;
            }
            case NATIVE_HANDLES -> {
                NativeHandle[] sourceValues = (NativeHandle[]) source.valueSlots;
                ((NativeHandle[]) target.valueSlots)[targetIndex] = sourceValues[sourceIndex];
                sourceValues[sourceIndex] = null;
            }
            case CONSTANT -> {
            }
        }
    }

    private void clearValue(Table table, int index) {
        switch (valueLayout) {
            case OBJECT_REFERENCES -> ((Object[]) table.valueSlots)[index] = null;
            case NATIVE_HANDLES -> ((NativeHandle[]) table.valueSlots)[index] = null;
            case CONSTANT -> {
            }
        }
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

    private int hashHandle(NativeHandle keyHandle) {
        return hashOverride == null
                ? byteStore.sipHash(keyHandle, hashSeed)
                : hashOverride.applyAsInt(byteStore.toByteArray(keyHandle));
    }

    private void requireOwnedKeys() {
        if (!ownsKeys) {
            throw new IllegalStateException("byte-array insertion requires an owning native byte-map");
        }
    }

    private static boolean timeLimitReached(long startedAt, long timeLimitNanos) {
        return timeLimitNanos != Long.MAX_VALUE && System.nanoTime() - startedAt >= timeLimitNanos;
    }

    private static long heapBytesForCapacity(int capacity, ValueLayout valueLayout) {
        long keyArray = ARRAY_HEADER_BYTES + (long) capacity * REFERENCE_BYTES;
        long valueArray = valueLayout.arrayHeapBytes(capacity);
        long hashArray = ARRAY_HEADER_BYTES + (long) capacity * Integer.BYTES;
        long stateArray = ARRAY_HEADER_BYTES + capacity;
        return TABLE_OBJECT_BYTES + keyArray + valueArray + hashArray + stateArray;
    }

    private int replacementTopologyCapacity(int addedCount) {
        if (addedCount == 0) {
            return 0;
        }
        long finalSize = (long) size + addedCount;
        if (finalSize > HashCapacityPolicy.MAX_CAPACITY) {
            throw new NativeCapacityExceededException("hash table capacity limit reached: " + finalSize);
        }
        if (old == null) {
            int reusedTombstones = Math.min(active.tombstones, addedCount);
            int projectedFilled = active.filled + addedCount - reusedTombstones;
            int projectedSize = active.size + addedCount;
            int projectedTombstones = active.tombstones - reusedTombstones;
            HashCapacityPolicy.Decision decision = HashCapacityPolicy.nextAction(
                    active.capacity,
                    projectedSize,
                    projectedFilled,
                    projectedTombstones
            );
            return decision.action() == HashCapacityPolicy.Action.NONE
                    ? 0
                    : decision.targetCapacity();
        }

        int capacity = active.capacity;
        while (finalSize > capacity - capacity / 4L) {
            if (capacity == HashCapacityPolicy.MAX_CAPACITY) {
                throw new NativeCapacityExceededException("hash table capacity limit reached: " + capacity);
            }
            capacity <<= 1;
        }
        return capacity == active.capacity ? 0 : capacity;
    }

    private Table buildReplacementTopology(
            StagedPut<V>[] puts,
            int[] hashes,
            NativeHandle[] newKeyHandles,
            int capacity
    ) {
        Table replacement = new Table(capacity, valueLayout);
        copyFilledSlots(active, replacement);
        copyFilledSlots(old, replacement);
        for (int index = 0; index < puts.length; index++) {
            StagedPut<V> put = puts[index];
            if (put.expectedPresent) {
                int replacementIndex = findIndex(replacement, put.keyBytes, hashes[index]);
                if (replacementIndex < 0) {
                    throw new IllegalStateException("prepared native byte-map key is missing from replacement topology");
                }
                writeValue(replacement, replacementIndex, put.nextValue);
                continue;
            }
            int replacementIndex = findInsertionIndex(replacement, hashes[index]);
            writeFilled(
                    replacement,
                    replacementIndex,
                    newKeyHandles[index],
                    put.nextValue,
                    hashes[index]
            );
        }
        return replacement;
    }

    private void copyFilledSlots(Table source, Table target) {
        if (source == null) {
            return;
        }
        for (int index = 0; index < source.capacity; index++) {
            if (source.states[index] != STATE_FILLED) {
                continue;
            }
            int targetIndex = findInsertionIndex(target, source.hashes[index]);
            writeFilled(
                    target,
                    targetIndex,
                    source.keyHandles[index],
                    valueAt(source, index),
                    source.hashes[index]
            );
        }
    }

    private static long preparedMutationHeapBytes(
            int putCount,
            int topologyCapacity,
            ValueLayout valueLayout
    ) {
        long referenceArrayBytes = ARRAY_HEADER_BYTES + (long) putCount * REFERENCE_BYTES;
        long intArrayBytes = ARRAY_HEADER_BYTES + (long) putCount * Integer.BYTES;
        long handleArrayBytes = ARRAY_HEADER_BYTES + (long) putCount * REFERENCE_BYTES;
        long duplicateIndexBytes = ARRAY_HEADER_BYTES
                + (long) PreparedMutation.duplicateIndexCapacity(putCount) * Integer.BYTES;
        long topologyBytes = topologyCapacity == 0
                ? 0L
                : heapBytesForCapacity(topologyCapacity, valueLayout);
        long arrays = referenceArrayBytes * 2L
                + intArrayBytes * 2L
                + handleArrayBytes
                + duplicateIndexBytes;
        return addSaturating(PREPARED_MUTATION_OBJECT_BYTES, addSaturating(arrays, topologyBytes));
    }

    @FunctionalInterface
    public interface EntryConsumer<V> {
        void accept(NativeHandle keyHandle, V value);
    }

    static record StagedPut<V>(
            byte[] keyBytes,
            V nextValue,
            boolean expectedPresent,
            NativeHandle borrowedKeyHandle
    ) {
        StagedPut(byte[] keyBytes, V nextValue, boolean expectedPresent) {
            this(keyBytes, nextValue, expectedPresent, null);
        }

        static <V> StagedPut<V> borrowed(
                byte[] keyBytes,
                NativeHandle keyHandle,
                V nextValue,
                boolean expectedPresent
        ) {
            Objects.requireNonNull(keyHandle, "keyHandle");
            if (keyHandle.isNull()) {
                throw new IllegalArgumentException("keyHandle must not be null");
            }
            return new StagedPut<>(keyBytes, nextValue, expectedPresent, keyHandle);
        }

        StagedPut {
            Objects.requireNonNull(keyBytes, "keyBytes");
        }
    }

    static final class PreparedMutation<V> implements AutoCloseable {
        private static final int MAX_INDEX_CAPACITY = 1 << 30;

        private final NativeByteMap<V> owner;
        private final StagedPut<V>[] puts;
        private final int[] hashes;
        private final int[] sourceLocations;
        private final NativeHandle[] keyHandles;
        private final Object[] previousValues;
        private final Table sourceActive;
        private final Table sourceOld;
        private final int sourceSize;
        private final long sourceGeneration;
        private final long sourceContentGeneration;
        private final int addedCount;
        private final long stagedNativeBytes;
        private final int topologyCapacity;
        private Table replacement;
        private boolean committed;
        private boolean released;
        private boolean validated;

        private PreparedMutation(NativeByteMap<V> owner, StagedPut<V>[] puts) {
            this.owner = Objects.requireNonNull(owner, "owner");
            this.puts = Objects.requireNonNull(puts, "puts");
            this.hashes = new int[puts.length];
            this.sourceLocations = new int[puts.length];
            this.keyHandles = new NativeHandle[puts.length];
            this.previousValues = new Object[puts.length];
            this.sourceActive = owner.active;
            this.sourceOld = owner.old;
            this.sourceSize = owner.size;
            this.sourceGeneration = owner.generation;
            this.sourceContentGeneration = owner.contentGeneration;

            int stagedAddedCount = inspectSource();
            this.addedCount = stagedAddedCount;
            this.topologyCapacity = owner.replacementTopologyCapacity(stagedAddedCount);

            long allocatedBytes = 0L;
            try {
                for (int index = 0; index < puts.length; index++) {
                    if (sourceLocations[index] != 0) {
                        continue;
                    }
                    NativeHandle keyHandle = owner.ownsKeys
                            ? owner.byteStore.store(puts[index].keyBytes, owner.keyKind)
                            : puts[index].borrowedKeyHandle;
                    keyHandles[index] = keyHandle;
                    if (owner.ownsKeys) {
                        allocatedBytes = addSaturating(
                                allocatedBytes,
                                owner.byteStore.allocatedBytes(keyHandle)
                        );
                    }
                }
                if (topologyCapacity != 0) {
                    replacement = owner.buildReplacementTopology(
                            puts,
                            hashes,
                            keyHandles,
                            topologyCapacity
                    );
                }
            } catch (RuntimeException | Error failure) {
                releaseUncommittedKeys(failure);
                throw failure;
            }
            this.stagedNativeBytes = allocatedBytes;
        }

        int addedCount() {
            return addedCount;
        }

        @SuppressWarnings("unchecked")
        V previousValue(int index) {
            ensureIndex(index);
            return (V) previousValues[index];
        }

        long stagedHeapBytes() {
            ensurePrepared();
            return preparedMutationHeapBytes(puts.length, topologyCapacity, owner.valueLayout);
        }

        long targetHeapBytes() {
            ensurePrepared();
            return replacement == null ? owner.heapBytes() : replacement.heapBytes;
        }

        void commit() {
            validateForCommit();
            commitValidated();
        }

        void validateForCommit() {
            ensurePrepared();
            validateCurrentSource();
            validated = true;
        }

        void commitValidated() {
            ensurePrepared();
            if (!validated) {
                throw new IllegalStateException("prepared native byte-map mutation was not validated");
            }

            // topology 在 prepare 阶段已经填好；这里发布数组引用，不再触发 native 分配。
            if (replacement != null) {
                owner.active = replacement;
                owner.old = null;
                owner.rehashCursor = 0;
                owner.size = sourceSize + addedCount;
                owner.nativeBytes += stagedNativeBytes;
                if (sourceOld != null) {
                    owner.completedRehashes++;
                }
                owner.generation++;
                owner.contentGeneration++;
                owner.recordMaintenanceDebt();
                replacement = null;
                committed = true;
                return;
            }

            for (int index = 0; index < puts.length; index++) {
                int location = sourceLocations[index];
                if (location == 0) {
                    continue;
                }
                Table table = location > 0 ? sourceActive : sourceOld;
                int slot = decodeLocation(location);
                owner.writeValue(table, slot, puts[index].nextValue);
            }
            for (int index = 0; index < puts.length; index++) {
                if (sourceLocations[index] == 0) {
                    owner.insertActive(keyHandles[index], puts[index].nextValue, hashes[index]);
                }
            }
            owner.nativeBytes += stagedNativeBytes;
            owner.contentGeneration++;
            committed = true;
        }

        void releaseSuperseded() {
            if (!committed) {
                throw new IllegalStateException("prepared native byte-map mutation is not committed");
            }
            if (released) {
                return;
            }
            owner.notifyHeapChanged();
            released = true;
        }

        private int inspectSource() {
            int[] stagedKeyIndex = new int[duplicateIndexCapacity(puts.length)];
            int added = 0;
            for (int index = 0; index < puts.length; index++) {
                StagedPut<V> put = puts[index];
                int hash = owner.hash(put.keyBytes);
                hashes[index] = hash;
                rejectDuplicate(stagedKeyIndex, index, hash);

                int activeIndex = owner.findIndex(sourceActive, put.keyBytes, hash);
                if (activeIndex >= 0) {
                    sourceLocations[index] = encodeActiveLocation(activeIndex);
                    keyHandles[index] = sourceActive.keyHandles[activeIndex];
                    previousValues[index] = owner.valueAt(sourceActive, activeIndex);
                } else {
                    int oldIndex = owner.findIndex(sourceOld, put.keyBytes, hash);
                    if (oldIndex >= 0) {
                        sourceLocations[index] = encodeOldLocation(oldIndex);
                        keyHandles[index] = sourceOld.keyHandles[oldIndex];
                        previousValues[index] = owner.valueAt(sourceOld, oldIndex);
                    }
                }

                boolean present = sourceLocations[index] != 0;
                if (present != put.expectedPresent) {
                    throw new IllegalStateException("prepared native byte-map source presence changed");
                }
                if (!present) {
                    added++;
                }
            }
            return added;
        }

        private void rejectDuplicate(int[] stagedKeyIndex, int candidateIndex, int hash) {
            if (stagedKeyIndex.length == 0) {
                return;
            }
            int mask = stagedKeyIndex.length - 1;
            int slot = spread(hash) & mask;
            while (stagedKeyIndex[slot] != 0) {
                int previousIndex = stagedKeyIndex[slot] - 1;
                if (hashes[previousIndex] == hash
                        && Arrays.equals(puts[previousIndex].keyBytes, puts[candidateIndex].keyBytes)) {
                    throw new IllegalArgumentException("prepared native byte-map puts contain a duplicate key");
                }
                slot = (slot + 1) & mask;
            }
            stagedKeyIndex[slot] = candidateIndex + 1;
        }

        private void validateCurrentSource() {
            if (owner.active != sourceActive
                    || owner.old != sourceOld
                    || owner.size != sourceSize
                    || owner.generation != sourceGeneration
                    || owner.contentGeneration != sourceContentGeneration) {
                throw new IllegalStateException("prepared native byte-map source topology changed");
            }
            for (int index = 0; index < puts.length; index++) {
                int location = sourceLocations[index];
                if (location == 0) {
                    if (owner.findIndex(sourceActive, puts[index].keyBytes, hashes[index]) >= 0
                            || owner.findIndex(sourceOld, puts[index].keyBytes, hashes[index]) >= 0) {
                        throw new IllegalStateException("prepared native byte-map source membership changed");
                    }
                    continue;
                }
                Table table = location > 0 ? sourceActive : sourceOld;
                int slot = decodeLocation(location);
                if (table == null
                        || table.states[slot] != STATE_FILLED
                        || table.hashes[slot] != hashes[index]
                        || !Objects.equals(table.keyHandles[slot], keyHandles[index])
                        || !Objects.equals(owner.valueAt(table, slot), previousValues[index])) {
                    throw new IllegalStateException("prepared native byte-map source entry changed");
                }
            }
        }

        private void releaseUncommittedKeys(Throwable failure) {
            for (int index = 0; index < keyHandles.length; index++) {
                if (!owner.ownsKeys
                        || sourceLocations[index] != 0
                        || keyHandles[index] == null) {
                    continue;
                }
                try {
                    owner.byteStore.release(keyHandles[index]);
                } catch (RuntimeException | Error releaseFailure) {
                    failure.addSuppressed(releaseFailure);
                } finally {
                    keyHandles[index] = null;
                }
            }
        }

        private void ensurePrepared() {
            if (committed || released) {
                throw new IllegalStateException("prepared native byte-map mutation is closed");
            }
        }

        private void ensureIndex(int index) {
            if (index < 0 || index >= previousValues.length) {
                throw new IndexOutOfBoundsException(index);
            }
        }

        @Override
        public void close() {
            if (committed || released) {
                return;
            }
            released = true;
            RuntimeException cleanupFailure = new RuntimeException(
                    "prepared native byte-map cleanup failed"
            );
            releaseUncommittedKeys(cleanupFailure);
            replacement = null;
            if (cleanupFailure.getSuppressed().length != 0) {
                throw cleanupFailure;
            }
        }

        private static int encodeActiveLocation(int index) {
            return index + 1;
        }

        private static int encodeOldLocation(int index) {
            return -(index + 1);
        }

        private static int decodeLocation(int location) {
            return Math.abs(location) - 1;
        }

        private static int duplicateIndexCapacity(int entryCount) {
            if (entryCount == 0) {
                return 0;
            }
            long required = Math.max(2L, (long) entryCount * 2L);
            if (required > MAX_INDEX_CAPACITY) {
                throw new IllegalArgumentException("too many native byte-map puts to prepare");
            }
            int capacity = 2;
            while (capacity < required) {
                capacity <<= 1;
            }
            return capacity;
        }

        private static int spread(int hash) {
            return hash ^ (hash >>> 16);
        }
    }

    public record ScanResult(
            ScanCursorV2 startCursor,
            ScanCursorV2 nextCursor,
            long inspectedSlots,
            long tableGeneration
    ) {
    }

    @FunctionalInterface
    public interface ScanConsumer<V> {
        boolean accept(NativeHandle keyHandle, V value);
    }

    public final class StagedResize implements AutoCloseable {
        private final Table source;
        private Table replacement;
        private boolean terminal;

        private StagedResize(Table source, Table replacement) {
            this.source = Objects.requireNonNull(source, "source");
            this.replacement = Objects.requireNonNull(replacement, "replacement");
        }

        public long stagedHeapBytes() {
            ensureActive();
            return replacement.heapBytes;
        }

        private Table publish() {
            ensureActive();
            Table published = replacement;
            replacement = null;
            terminal = true;
            return published;
        }

        private void ensureActive() {
            if (terminal || replacement == null) {
                throw new IllegalStateException("staged native byte-map resize is closed");
            }
        }

        @Override
        public void close() {
            if (terminal) {
                return;
            }
            terminal = true;
            replacement = null;
        }
    }

    private enum ValueLayout {
        OBJECT_REFERENCES,
        NATIVE_HANDLES,
        CONSTANT;

        private Object allocateSlots(int capacity) {
            return switch (this) {
                case OBJECT_REFERENCES -> new Object[capacity];
                case NATIVE_HANDLES -> new NativeHandle[capacity];
                case CONSTANT -> null;
            };
        }

        private long arrayHeapBytes(int capacity) {
            return switch (this) {
                case OBJECT_REFERENCES -> ARRAY_HEADER_BYTES + (long) capacity * REFERENCE_BYTES;
                case NATIVE_HANDLES -> ARRAY_HEADER_BYTES + (long) capacity * REFERENCE_BYTES;
                case CONSTANT -> 0L;
            };
        }
    }

    private static final class Table {
        private final int capacity;
        private final byte[] states;
        private final int[] hashes;
        private final NativeHandle[] keyHandles;
        private final Object valueSlots;
        private final long heapBytes;
        private int size;
        private int filled;
        private int tombstones;

        private Table(int capacity, ValueLayout valueLayout) {
            if (capacity < HashCapacityPolicy.MIN_CAPACITY
                    || capacity > HashCapacityPolicy.MAX_CAPACITY
                    || (capacity & (capacity - 1)) != 0) {
                throw new IllegalArgumentException("invalid native byte-map capacity: " + capacity);
            }
            this.capacity = capacity;
            this.states = new byte[capacity];
            this.hashes = new int[capacity];
            this.keyHandles = new NativeHandle[capacity];
            this.valueSlots = valueLayout.allocateSlots(capacity);
            this.heapBytes = heapBytesForCapacity(capacity, valueLayout);
        }
    }
}
