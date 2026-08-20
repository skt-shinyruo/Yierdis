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
import yier.bubu.redis.storage.memory.internal.hash.OpenAddressingTopology;
import yier.bubu.redis.storage.memory.internal.hash.OpenAddressingTopology.Location;
import yier.bubu.redis.storage.memory.internal.hash.OpenAddressingTopology.ProbeResult;
import yier.bubu.redis.storage.memory.internal.hash.OpenAddressingTopology.SlotState;
import yier.bubu.redis.storage.memory.internal.hash.OpenAddressingTopology.TableSide;
import yier.bubu.redis.storage.memory.internal.hash.SipHash24;

public final class NativeByteMap<V> implements AutoCloseable, HashTableMaintenanceRegistry.Participant {
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long REFERENCE_BYTES = 8L;
    private static final long TABLE_OBJECT_BYTES = 48L;
    private static final long REPLACEMENT_OBJECT_BYTES = 32L;
    private static final long PREPARED_MUTATION_OBJECT_BYTES = 128L;
    private static final long SOURCE_LOCATION_OBJECT_BYTES = 32L;
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
    private final OpenAddressingTopology topology;
    private Table active;
    private Table old;
    private long nativeBytes;
    private long contentGeneration;
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
        this.topology = new OpenAddressingTopology(HashCapacityPolicy.MIN_CAPACITY);
        this.active = new Table(HashCapacityPolicy.MIN_CAPACITY, valueLayout);
    }

    public int size() {
        return topology.metrics().size();
    }

    public boolean containsKey(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        int hash = hash(keyBytes);
        return probe(keyBytes, hash).found();
    }

    public V get(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        int hash = hash(keyBytes);
        ProbeResult probe = probe(keyBytes, hash);
        return probe.found() ? valueAt(table(probe.location().table()), probe.location().slot()) : null;
    }

    public V put(byte[] keyBytes, V value) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        requireOwnedKeys();
        advanceRehashOnWrite();
        int hash = hash(keyBytes);
        ProbeResult probe = probe(keyBytes, hash);
        if (probe.found()) {
            Location location = probe.location();
            V previous = valueAt(table(location.table()), location.slot());
            int activeIndex = location.table() == TableSide.ACTIVE
                    ? location.slot()
                    : moveOldSlotToActive(location.slot());
            writeValue(active, activeIndex, value);
            contentGeneration++;
            return previous;
        }

        Replacement staged = stageTableForInsert(hash);
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
        ProbeResult probe = probe(keyHandle, hash);
        if (probe.found()) {
            Location location = probe.location();
            V previous = valueAt(table(location.table()), location.slot());
            int activeIndex = location.table() == TableSide.ACTIVE
                    ? location.slot()
                    : moveOldSlotToActive(location.slot());
            writeValue(active, activeIndex, value);
            contentGeneration++;
            return previous;
        }

        Replacement staged = stageTableForInsert(hash);
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
        return preparedMutationHeapBytes(putCount, addedCount, topologyCapacity, valueLayout);
    }

    public V replace(byte[] keyBytes, V value) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        advanceRehashOnWrite();
        int hash = hash(keyBytes);
        ProbeResult probe = probe(keyBytes, hash);
        if (!probe.found()) {
            return null;
        }
        Location location = probe.location();
        V previous = valueAt(table(location.table()), location.slot());
        int activeIndex = location.table() == TableSide.ACTIVE
                ? location.slot()
                : moveOldSlotToActive(location.slot());
        writeValue(active, activeIndex, value);
        contentGeneration++;
        return previous;
    }

    public V remove(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        advanceRehashOnWrite();
        int hash = hash(keyBytes);
        ProbeResult probe = probe(keyBytes, hash);
        if (!probe.found()) {
            return null;
        }
        Location location = probe.location();
        V previous = valueAt(table(location.table()), location.slot());
        remove(location, hash);
        contentGeneration++;
        return previous;
    }

    public V remove(NativeHandle keyHandle) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        advanceRehashOnWrite();
        Location location = findHandleLocation(keyHandle);
        if (location == null) {
            return null;
        }
        V previous = valueAt(table(location.table()), location.slot());
        remove(location, topology.hashAt(location));
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
        HashTableMetrics metrics = topology.metrics();
        if (metrics.size() == 0) {
            return new ScanResult(start, ScanCursorV2.start(), 0L, topology.metrics().generation());
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
                return new ScanResult(start, ScanCursorV2.start(), inspected, topology.metrics().generation());
            }

            if (inspected >= maxSteps) {
                return new ScanResult(start, cursorFor(phase, position), inspected, topology.metrics().generation());
            }

            int index = (int) position;
            position++;
            inspected++;
            if (!acceptScanSlot(table, phase, index, consumer)) {
                if (position >= table.capacity) {
                    if (phase == 0 && old != null) {
                        return new ScanResult(start, cursorFor(1, 0L), inspected, topology.metrics().generation());
                    }
                    return new ScanResult(start, ScanCursorV2.start(), inspected, topology.metrics().generation());
                }
                return new ScanResult(start, cursorFor(phase, position), inspected, topology.metrics().generation());
            }
        }
    }

    public void clear() {
        releaseTableKeys(active, TableSide.ACTIVE);
        releaseTableKeys(old, TableSide.OLD);
        active = new Table(HashCapacityPolicy.MIN_CAPACITY, valueLayout);
        old = null;
        topology.reset(HashCapacityPolicy.MIN_CAPACITY);
        nativeBytes = 0L;
        maintenanceDebt = false;
        refreshMaintenanceRegistration();
        contentGeneration++;
        notifyHeapChanged();
    }

    public long nativeBytes() {
        return nativeBytes;
    }

    public long heapBytes() {
        return active.heapBytes
                + (old == null ? 0L : old.heapBytes)
                + topology.heapEstimatedBytes();
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
        long tableBytes = committedHeapBytesForCapacity(capacity, valueLayout);
        return tableBytes > Long.MAX_VALUE - tableBytes ? Long.MAX_VALUE : tableBytes + tableBytes;
    }

    static long heapUpperBoundForNativeHandleValues(long expectedEntries) {
        return heapUpperBoundForEntries(expectedEntries, ValueLayout.NATIVE_HANDLES);
    }

    static long heapUpperBoundForConstantValues(long expectedEntries) {
        return heapUpperBoundForEntries(expectedEntries, ValueLayout.CONSTANT);
    }

    public long estimatedInsertHeapGrowthBytes() {
        HashTableMetrics metrics = topology.metrics();
        if (metrics.rehashing()) {
            return 0L;
        }
        int projectedFilled = Math.min(metrics.capacity(), metrics.filledSlots() + 1);
        int projectedSize = metrics.size() + 1;
        int projectedTombstones = projectedFilled - projectedSize;
        if (projectedTombstones < 0) {
            return 0L;
        }
        HashCapacityPolicy.Decision decision = HashCapacityPolicy.nextAction(
                metrics.capacity(),
                projectedSize,
                projectedFilled,
                projectedTombstones
        );
        return decision.action() == HashCapacityPolicy.Action.NONE
                ? 0L
                : stagedReplacementHeapBytesForCapacity(decision.targetCapacity(), valueLayout);
    }

    public HashTableMetrics metrics() {
        return topology.metrics();
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
                : stagedReplacementHeapBytesForCapacity(decision.targetCapacity(), valueLayout);
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
        return new StagedResize(active, emptyReplacement(decision.targetCapacity()));
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
        HashTableWorkResult result = topology.advanceRehash(budget, (sourceIndex, targetIndex) -> {
            NativeHandle keyHandle = old.keyHandles[sourceIndex];
            moveValue(old, sourceIndex, active, targetIndex);
            active.keyHandles[targetIndex] = keyHandle;
        });
        if (result.rehashComplete()) {
            old = null;
            recordMaintenanceDebt();
            notifyHeapChanged();
        }
        return result;
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
        TableSide side = table == active ? TableSide.ACTIVE : TableSide.OLD;
        for (int i = 0; i < table.capacity; i++) {
            if (topology.slotState(side, i) == SlotState.FILLED) {
                consumer.accept(keyHandleAt(table, i), valueAt(table, i));
            }
        }
    }

    private boolean acceptScanSlot(Table table, int phase, int index, ScanConsumer<V> consumer) {
        SlotState state = topology.slotState(phase == 0 ? TableSide.ACTIVE : TableSide.OLD, index);
        if (state == SlotState.FILLED) {
            return consumer.accept(keyHandleAt(table, index), valueAt(table, index));
        }
        if (phase != 1 || state != SlotState.MIGRATED_SCAN_SHADOW) {
            return true;
        }

        // shadow 只保留迁移定位信息；active 中的 value 可能已替换，旧 value 甚至已由上层释放。
        NativeHandle keyHandle = table.keyHandles[index];
        ProbeResult probe = probeStoredKey(
                keyHandle,
                topology.hashAt(new Location(TableSide.OLD, index))
        );
        return !probe.found()
                || consumer.accept(keyHandleAt(active, probe.location().slot()), valueAt(active, probe.location().slot()));
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
        return (int) (topology.metrics().generation() & 0x1fff_ffffL);
    }

    private void releaseTableKeys(Table table, TableSide side) {
        if (table == null) {
            return;
        }
        for (int i = 0; i < table.capacity; i++) {
            if (topology.slotState(side, i) == SlotState.FILLED) {
                if (ownsKeys) {
                    byteStore.release(table.keyHandles[i]);
                }
                table.keyHandles[i] = null;
                clearValue(table, i);
            }
        }
    }

    private Replacement stageTableForInsert(int hash) {
        if (old != null) {
            return null;
        }
        HashTableMetrics metrics = topology.metrics();
        int insertionIndex = topology.probe(hash, ignored -> false).location().slot();
        SlotState previousState = topology.slotState(TableSide.ACTIVE, insertionIndex);
        int projectedSize = metrics.size() + 1;
        int projectedFilled = metrics.filledSlots() + (previousState == SlotState.EMPTY ? 1 : 0);
        int projectedTombstones = metrics.tombstones() - (previousState == SlotState.TOMBSTONE ? 1 : 0);
        HashCapacityPolicy.Decision decision = HashCapacityPolicy.nextAction(
                metrics.capacity(),
                projectedSize,
                projectedFilled,
                projectedTombstones
        );
        return decision.action() == HashCapacityPolicy.Action.NONE
                ? null
                : emptyReplacement(decision.targetCapacity());
    }

    private Replacement emptyReplacement(int capacity) {
        return new Replacement(
                new Table(capacity, valueLayout),
                new OpenAddressingTopology(capacity)
        );
    }

    private void publishStagedTable(Replacement staged) {
        if (old != null) {
            throw new IllegalStateException("cannot start a second native byte-map rehash");
        }
        Replacement replacement = Objects.requireNonNull(staged, "staged");
        Table previous = active;
        topology.beginRehash(replacement.topology);
        old = previous;
        active = replacement.table;
        maintenanceDebt = true;
        refreshMaintenanceRegistration();
        notifyHeapChanged();
    }

    private void insertActive(NativeHandle keyHandle, V value, int hash) {
        ProbeResult probe = topology.probe(hash, ignored -> false);
        if (probe.found()) {
            throw new IllegalStateException("native byte-map key already exists during insert");
        }
        int index = probe.location().slot();
        writeValue(active, index, value);
        active.keyHandles[index] = keyHandle;
        topology.occupyActive(index, hash);
    }

    private int moveOldSlotToActive(int oldIndex) {
        return topology.promoteOld(oldIndex, (sourceIndex, targetIndex) -> {
            NativeHandle keyHandle = old.keyHandles[sourceIndex];
            moveValue(old, sourceIndex, active, targetIndex);
            active.keyHandles[targetIndex] = keyHandle;
        });
    }

    private void remove(Location location, int hash) {
        Table table = table(location.table());
        int index = location.slot();
        NativeHandle keyHandle = table.keyHandles[index];
        if (location.table() == TableSide.ACTIVE) {
            invalidateOldShadow(keyHandle, hash);
        }
        if (ownsKeys) {
            nativeBytes -= byteStore.allocatedBytes(keyHandle);
            byteStore.release(keyHandle);
        }
        table.keyHandles[index] = null;
        clearValue(table, index);
        topology.remove(location);
        recordMaintenanceDebt();
    }

    private HashCapacityPolicy.Decision maintenanceDecision() {
        HashTableMetrics metrics = topology.metrics();
        return HashCapacityPolicy.nextAction(
                metrics.capacity(),
                metrics.size(),
                metrics.filledSlots(),
                metrics.tombstones()
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
        int index = topology.invalidateOldShadow(
                hash,
                location -> keyHandle.equals(old.keyHandles[location.slot()])
        );
        if (index >= 0) {
            old.keyHandles[index] = null;
            clearValue(old, index);
        }
    }

    private ProbeResult probe(byte[] keyBytes, int hash) {
        return topology.probe(
                hash,
                location -> byteStore.equalsBytes(
                        table(location.table()).keyHandles[location.slot()],
                        keyBytes
                )
        );
    }

    private ProbeResult probe(NativeHandle keyHandle, int hash) {
        return topology.probe(
                hash,
                location -> byteStore.compareLex(
                        table(location.table()).keyHandles[location.slot()],
                        keyHandle
                ) == 0
        );
    }

    private ProbeResult probeStoredKey(NativeHandle keyHandle, int hash) {
        return topology.probe(
                hash,
                location -> keyHandle.equals(table(location.table()).keyHandles[location.slot()])
        );
    }

    private Location findHandleLocation(NativeHandle keyHandle) {
        for (TableSide side : TableSide.values()) {
            Table table = table(side);
            if (table == null) {
                continue;
            }
            for (int slot = 0; slot < table.capacity; slot++) {
                if (topology.slotState(side, slot) == SlotState.FILLED
                        && byteStore.compareLex(table.keyHandles[slot], keyHandle) == 0) {
                    return new Location(side, slot);
                }
            }
        }
        return null;
    }

    private Table table(TableSide side) {
        return side == TableSide.ACTIVE ? active : old;
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

    private static long payloadHeapBytesForCapacity(int capacity, ValueLayout valueLayout) {
        long keyArray = ARRAY_HEADER_BYTES + (long) capacity * REFERENCE_BYTES;
        long valueArray = valueLayout.arrayHeapBytes(capacity);
        return TABLE_OBJECT_BYTES + keyArray + valueArray;
    }

    private static long committedHeapBytesForCapacity(int capacity, ValueLayout valueLayout) {
        return payloadHeapBytesForCapacity(capacity, valueLayout)
                + OpenAddressingTopology.standaloneHeapEstimatedBytes(capacity);
    }

    private static long stagedReplacementHeapBytesForCapacity(int capacity, ValueLayout valueLayout) {
        return REPLACEMENT_OBJECT_BYTES + committedHeapBytesForCapacity(capacity, valueLayout);
    }

    private int replacementTopologyCapacity(int addedCount) {
        if (addedCount == 0) {
            return 0;
        }
        HashTableMetrics metrics = topology.metrics();
        long finalSize = (long) metrics.size() + addedCount;
        if (finalSize > HashCapacityPolicy.MAX_CAPACITY) {
            throw new NativeCapacityExceededException("hash table capacity limit reached: " + finalSize);
        }
        if (old == null) {
            int reusedTombstones = Math.min(metrics.tombstones(), addedCount);
            int projectedFilled = metrics.filledSlots() + addedCount - reusedTombstones;
            int projectedSize = metrics.size() + addedCount;
            int projectedTombstones = metrics.tombstones() - reusedTombstones;
            HashCapacityPolicy.Decision decision = HashCapacityPolicy.nextAction(
                    metrics.capacity(),
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

    private Replacement buildReplacementTopology(
            StagedPut<V>[] puts,
            int[] hashes,
            NativeHandle[] newKeyHandles,
            int capacity
    ) {
        Table replacementTable = new Table(capacity, valueLayout);
        OpenAddressingTopology replacementTopology = new OpenAddressingTopology(capacity);
        copyFilledSlots(TableSide.ACTIVE, active, replacementTable, replacementTopology);
        copyFilledSlots(TableSide.OLD, old, replacementTable, replacementTopology);
        for (int index = 0; index < puts.length; index++) {
            StagedPut<V> put = puts[index];
            ProbeResult probe = replacementTopology.probe(
                    hashes[index],
                    location -> byteStore.equalsBytes(
                            replacementTable.keyHandles[location.slot()],
                            put.keyBytes
                    )
            );
            if (put.expectedPresent) {
                if (!probe.found()) {
                    throw new IllegalStateException("prepared native byte-map key is missing from replacement topology");
                }
                writeValue(replacementTable, probe.location().slot(), put.nextValue);
                continue;
            }
            int replacementIndex = probe.location().slot();
            replacementTable.keyHandles[replacementIndex] = newKeyHandles[index];
            writeValue(replacementTable, replacementIndex, put.nextValue);
            replacementTopology.occupyActive(replacementIndex, hashes[index]);
        }
        return new Replacement(replacementTable, replacementTopology);
    }

    private void copyFilledSlots(
            TableSide sourceSide,
            Table source,
            Table target,
            OpenAddressingTopology targetTopology
    ) {
        if (source == null) {
            return;
        }
        for (int index = 0; index < source.capacity; index++) {
            if (topology.slotState(sourceSide, index) != SlotState.FILLED) {
                continue;
            }
            NativeHandle keyHandle = source.keyHandles[index];
            int hash = topology.hashAt(new Location(sourceSide, index));
            int targetIndex = targetTopology.probe(hash, ignored -> false).location().slot();
            target.keyHandles[targetIndex] = keyHandle;
            writeValue(target, targetIndex, valueAt(source, index));
            targetTopology.occupyActive(targetIndex, hash);
        }
    }

    private static long preparedMutationHeapBytes(
            int putCount,
            int addedCount,
            int topologyCapacity,
            ValueLayout valueLayout
    ) {
        long referenceArrayBytes = ARRAY_HEADER_BYTES + (long) putCount * REFERENCE_BYTES;
        long intArrayBytes = ARRAY_HEADER_BYTES + (long) putCount * Integer.BYTES;
        long handleArrayBytes = ARRAY_HEADER_BYTES + (long) putCount * REFERENCE_BYTES;
        long sourceLocationBytes = (long) (putCount - addedCount) * SOURCE_LOCATION_OBJECT_BYTES;
        long duplicateIndexBytes = ARRAY_HEADER_BYTES
                + (long) PreparedMutation.duplicateIndexCapacity(putCount) * Integer.BYTES;
        long topologyBytes = topologyCapacity == 0
                ? 0L
                : stagedReplacementHeapBytesForCapacity(topologyCapacity, valueLayout);
        long arrays = referenceArrayBytes * 3L
                + intArrayBytes
                + handleArrayBytes
                + duplicateIndexBytes;
        return addSaturating(
                PREPARED_MUTATION_OBJECT_BYTES,
                addSaturating(sourceLocationBytes, addSaturating(arrays, topologyBytes))
        );
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

    private enum SourceTable {
        ACTIVE,
        OLD,
        ABSENT
    }

    private record SourceLocation(SourceTable table, int slot) {
        private static final SourceLocation ABSENT = new SourceLocation(SourceTable.ABSENT, -1);

        SourceLocation {
            Objects.requireNonNull(table, "table");
            if (table == SourceTable.ABSENT ? slot != -1 : slot < 0) {
                throw new IllegalArgumentException("invalid native byte-map source location");
            }
        }

        static SourceLocation active(int slot) {
            return new SourceLocation(SourceTable.ACTIVE, slot);
        }

        static SourceLocation old(int slot) {
            return new SourceLocation(SourceTable.OLD, slot);
        }

        static SourceLocation absent() {
            return ABSENT;
        }

        boolean present() {
            return table != SourceTable.ABSENT;
        }
    }

    private record Replacement(Table table, OpenAddressingTopology topology) {
        private Replacement {
            Objects.requireNonNull(table, "table");
            Objects.requireNonNull(topology, "topology");
        }

        private long committedHeapBytes() {
            return table.heapBytes + topology.heapEstimatedBytes();
        }

        private long stagedHeapBytes() {
            return REPLACEMENT_OBJECT_BYTES + committedHeapBytes();
        }
    }

    static final class PreparedMutation<V> implements AutoCloseable {
        private static final int MAX_INDEX_CAPACITY = 1 << 30;

        private final NativeByteMap<V> owner;
        private final StagedPut<V>[] puts;
        private final int[] hashes;
        private final SourceLocation[] sourceLocations;
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
        private Replacement replacement;
        private boolean committed;
        private boolean released;
        private boolean validated;

        private PreparedMutation(NativeByteMap<V> owner, StagedPut<V>[] puts) {
            this.owner = Objects.requireNonNull(owner, "owner");
            this.puts = Objects.requireNonNull(puts, "puts");
            this.hashes = new int[puts.length];
            this.sourceLocations = new SourceLocation[puts.length];
            Arrays.fill(this.sourceLocations, SourceLocation.absent());
            this.keyHandles = new NativeHandle[puts.length];
            this.previousValues = new Object[puts.length];
            this.sourceActive = owner.active;
            this.sourceOld = owner.old;
            this.sourceSize = owner.size();
            this.sourceGeneration = owner.metrics().generation();
            this.sourceContentGeneration = owner.contentGeneration;

            int stagedAddedCount = inspectSource();
            this.addedCount = stagedAddedCount;
            this.topologyCapacity = owner.replacementTopologyCapacity(stagedAddedCount);

            long allocatedBytes = 0L;
            try {
                for (int index = 0; index < puts.length; index++) {
                    if (sourceLocations[index].present()) {
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
            return preparedMutationHeapBytes(puts.length, addedCount, topologyCapacity, owner.valueLayout);
        }

        long targetHeapBytes() {
            ensurePrepared();
            return replacement == null ? owner.heapBytes() : replacement.committedHeapBytes();
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
                owner.topology.replaceActive(replacement.topology);
                owner.active = replacement.table;
                owner.old = null;
                owner.nativeBytes += stagedNativeBytes;
                owner.contentGeneration++;
                owner.recordMaintenanceDebt();
                replacement = null;
                committed = true;
                return;
            }

            for (int index = 0; index < puts.length; index++) {
                SourceLocation location = sourceLocations[index];
                if (!location.present()) {
                    continue;
                }
                Table table = location.table() == SourceTable.ACTIVE ? sourceActive : sourceOld;
                int slot = location.slot();
                owner.writeValue(table, slot, puts[index].nextValue);
            }
            for (int index = 0; index < puts.length; index++) {
                if (!sourceLocations[index].present()) {
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

                ProbeResult probe = owner.probe(put.keyBytes, hash);
                if (probe.found()) {
                    Location location = probe.location();
                    Table source = location.table() == TableSide.ACTIVE ? sourceActive : sourceOld;
                    sourceLocations[index] = location.table() == TableSide.ACTIVE
                            ? SourceLocation.active(location.slot())
                            : SourceLocation.old(location.slot());
                    keyHandles[index] = source.keyHandles[location.slot()];
                    previousValues[index] = owner.valueAt(source, location.slot());
                }

                boolean present = sourceLocations[index].present();
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
                    || owner.size() != sourceSize
                    || owner.metrics().generation() != sourceGeneration
                    || owner.contentGeneration != sourceContentGeneration) {
                throw new IllegalStateException("prepared native byte-map source topology changed");
            }
            for (int index = 0; index < puts.length; index++) {
                SourceLocation location = sourceLocations[index];
                if (!location.present()) {
                    if (owner.probe(puts[index].keyBytes, hashes[index]).found()) {
                        throw new IllegalStateException("prepared native byte-map source membership changed");
                    }
                    continue;
                }
                Table table = location.table() == SourceTable.ACTIVE ? sourceActive : sourceOld;
                int slot = location.slot();
                if (table == null
                        || owner.topology.slotState(
                                location.table() == SourceTable.ACTIVE ? TableSide.ACTIVE : TableSide.OLD,
                                slot
                        ) != SlotState.FILLED
                        || !Objects.equals(table.keyHandles[slot], keyHandles[index])
                        || !Objects.equals(owner.valueAt(table, slot), previousValues[index])) {
                    throw new IllegalStateException("prepared native byte-map source entry changed");
                }
            }
        }

        private void releaseUncommittedKeys(Throwable failure) {
            for (int index = 0; index < keyHandles.length; index++) {
                if (!owner.ownsKeys
                        || sourceLocations[index].present()
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
        private Replacement replacement;
        private boolean terminal;

        private StagedResize(Table source, Replacement replacement) {
            this.source = Objects.requireNonNull(source, "source");
            this.replacement = Objects.requireNonNull(replacement, "replacement");
        }

        public long stagedHeapBytes() {
            ensureActive();
            return replacement.stagedHeapBytes();
        }

        private Replacement publish() {
            ensureActive();
            Replacement published = replacement;
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
        private final NativeHandle[] keyHandles;
        private final Object valueSlots;
        private final long heapBytes;

        private Table(int capacity, ValueLayout valueLayout) {
            if (capacity < HashCapacityPolicy.MIN_CAPACITY
                    || capacity > HashCapacityPolicy.MAX_CAPACITY
                    || (capacity & (capacity - 1)) != 0) {
                throw new IllegalArgumentException("invalid native byte-map capacity: " + capacity);
            }
            this.capacity = capacity;
            this.keyHandles = new NativeHandle[capacity];
            this.valueSlots = valueLayout.allocateSlots(capacity);
            this.heapBytes = payloadHeapBytesForCapacity(capacity, valueLayout);
        }
    }
}
