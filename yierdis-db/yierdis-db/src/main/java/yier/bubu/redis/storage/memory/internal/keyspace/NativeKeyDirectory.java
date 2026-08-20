package yier.bubu.redis.storage.memory.internal.keyspace;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
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
import yier.bubu.redis.storage.memory.internal.key.AllocatorKeyHandle;

public final class NativeKeyDirectory implements AutoCloseable, HashTableMaintenanceRegistry.Participant {
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long REFERENCE_BYTES = 8L;
    private static final long TABLE_OBJECT_BYTES = 48L;
    private static final long REPLACEMENT_OBJECT_BYTES = 32L;
    private static final long DETACHED_ENTRIES_OBJECT_BYTES = 64L;
    private static final HashTableWorkBudget WRITE_REHASH_BUDGET = HashTableWorkBudget.of(2L, Long.MAX_VALUE);

    private final StableMemoryBackend allocator;
    private final HashSeed hashSeed;
    private final HashTableMaintenanceRegistry maintenanceRegistry;
    private final HashTableMaintenanceRegistry.Registration maintenanceRegistration;
    private final OpenAddressingTopology topology;
    private Table active;
    private Table old;
    private boolean maintenanceDebt;
    private DetachedEntries detachedHead;
    private DetachedEntries detachedTail;
    private long detachedHeapBytes;
    private long detachedEntryCount;
    private boolean closed;

    public NativeKeyDirectory(
            StableMemoryBackend allocator,
            HashSeed hashSeed,
            HashTableMaintenanceRegistry maintenanceRegistry
    ) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.hashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
        this.maintenanceRegistry = maintenanceRegistry;
        this.maintenanceRegistration = maintenanceRegistry == null ? null : maintenanceRegistry.registration(this);
        this.topology = new OpenAddressingTopology(HashCapacityPolicy.MIN_CAPACITY);
        this.active = new Table(HashCapacityPolicy.MIN_CAPACITY);
    }

    public synchronized int size() {
        return topology.metrics().size();
    }

    public synchronized long nativeBytes() {
        ensureOpen();
        return 0L;
    }

    public synchronized long heapBytes() {
        ensureOpen();
        return active.heapBytes
                + (old == null ? 0L : old.heapBytes)
                + topology.heapEstimatedBytes()
                + detachedHeapBytes;
    }

    public synchronized long estimatedInsertHeapGrowthBytes() {
        ensureOpen();
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
                : stagedReplacementHeapBytesForCapacity(decision.targetCapacity());
    }

    public synchronized HashTableMetrics metrics() {
        ensureOpen();
        return topology.metrics();
    }

    public synchronized long tableGeneration() {
        ensureOpen();
        return topology.metrics().generation();
    }

    @Override
    public synchronized boolean hasMaintenanceDebt() {
        ensureOpen();
        return maintenanceDebt || old != null;
    }

    public synchronized StagedResize stageMaintenanceResize() {
        ensureOpen();
        if (topology.metrics().rehashing()) {
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

    @Override
    public synchronized long estimatedMaintenanceGrowthBytes() {
        ensureOpen();
        if (topology.metrics().rehashing()) {
            return 0L;
        }
        HashCapacityPolicy.Decision decision = maintenanceDecision();
        return decision.action() == HashCapacityPolicy.Action.NONE
                ? 0L
                : stagedReplacementHeapBytesForCapacity(decision.targetCapacity());
    }

    @Override
    public synchronized HashTableMaintenanceRegistry.MaintenancePreparation prepareMaintenance() {
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
                    throw new IllegalStateException("staged key-directory maintenance resize is closed");
                }
                return pending;
            }
        };
    }

    public synchronized void publishStagedResize(StagedResize staged) {
        Objects.requireNonNull(staged, "staged");
        ensureOpen();
        staged.ensureActive();
        if (topology.metrics().rehashing() || active != staged.source) {
            throw new IllegalStateException("staged key-directory resize is no longer current");
        }
        publishStagedDirectory(staged.publish());
    }

    @Override
    public synchronized HashTableWorkResult advanceRehash(HashTableWorkBudget budget) {
        Objects.requireNonNull(budget, "budget");
        ensureOpen();
        if (!topology.metrics().rehashing()) {
            return new HashTableWorkResult(0L, 0L, true, HashTableWorkResult.StopReason.NOT_REHASHING);
        }
        HashTableWorkResult result = topology.advanceRehash(budget, (sourceIndex, targetIndex) -> {
            NativeHandle keyHandle = old.keyHandles[sourceIndex];
            active.keyHandles[targetIndex] = keyHandle;
            active.entryHandles[targetIndex] = old.entryHandles[sourceIndex];
            // old shadow 只保留 key 用于 scan 定位；清空 entry 后，detach 可按 ownership marker 精确回收一次。
            old.entryHandles[sourceIndex] = null;
        });
        if (result.rehashComplete()) {
            old = null;
            recordMaintenanceDebt();
        }
        return result;
    }

    public synchronized EntryHandle get(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        ensureOpen();
        int hash = hash(keyBytes);
        ProbeResult probe = probe(keyBytes, hash);
        return probe.found() ? entryHandleAt(table(probe.location().table()), probe.location().slot()) : null;
    }

    public synchronized AllocatorKeyHandle getKeyHandle(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        ensureOpen();
        int hash = hash(keyBytes);
        ProbeResult probe = probe(keyBytes, hash);
        return probe.found() ? keyHandleAt(probe.location()) : null;
    }

    public synchronized AllocatorKeyHandle randomKeyHandle() {
        ensureOpen();
        if (topology.metrics().size() == 0) {
            return null;
        }
        AllocatorKeyHandle fromActive = randomKeyHandle(active, TableSide.ACTIVE);
        return fromActive != null ? fromActive : randomKeyHandle(old, TableSide.OLD);
    }

    public synchronized void forEachEntry(EntryConsumer consumer) {
        Objects.requireNonNull(consumer, "consumer");
        ensureOpen();
        forEachEntry(active, TableSide.ACTIVE, consumer);
        forEachEntry(old, TableSide.OLD, consumer);
    }

    public synchronized ScanCursorV2 scan(ScanCursorV2 cursor, int maxSteps, ScanConsumer consumer) {
        return scanWithWork(cursor, Math.max(0L, maxSteps), consumer).nextCursor();
    }

    public synchronized ScanResult scanWithWork(ScanCursorV2 cursor, long maxSteps, ScanConsumer consumer) {
        Objects.requireNonNull(cursor, "cursor");
        Objects.requireNonNull(consumer, "consumer");
        ensureOpen();
        if (maxSteps < 0L) {
            throw new IllegalArgumentException("maxSteps must be >= 0");
        }

        ScanCursorV2 start = normalizeScanCursor(cursor);
        if (topology.metrics().size() == 0) {
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
                    if (phase == 0 && topology.metrics().rehashing()) {
                        return new ScanResult(start, cursorFor(1, 0L), inspected, topology.metrics().generation());
                    }
                    return new ScanResult(start, ScanCursorV2.start(), inspected, topology.metrics().generation());
                }
                return new ScanResult(start, cursorFor(phase, position), inspected, topology.metrics().generation());
            }
        }
    }

    public StagedInsert stageInsert(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        byte[] stableKeyBytes = Arrays.copyOf(keyBytes, keyBytes.length);
        synchronized (this) {
            ensureOpen();
            advanceRehashOnWrite();
            int hash = hash(stableKeyBytes);
            if (probe(stableKeyBytes, hash).found()) {
                throw new IllegalStateException("native key already exists during staged insert");
            }
            Replacement staged = stageTableForInsert(hash);
            NativeHandle keyHandle = allocateKey(stableKeyBytes);
            return new StagedInsert(stableKeyBytes, hash, keyHandle, staged);
        }
    }

    public synchronized void publishStagedInsert(StagedInsert staged, EntryHandle entryHandle) {
        Objects.requireNonNull(staged, "staged");
        Objects.requireNonNull(entryHandle, "entryHandle");
        ensureOpen();
        staged.ensureActive();
        if (probe(staged.keyBytes, staged.hash).found()) {
            throw new IllegalStateException("native key appeared during staged insert");
        }
        if (staged.replacement != null) {
            publishStagedDirectory(staged.replacement);
        }
        recordMaintenanceDebt();
        NativeHandle keyHandle = staged.keyHandleForPublish();
        insertActive(keyHandle, entryHandle, staged.hash);
        staged.markPublished();
    }

    public synchronized EntryHandle remove(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        ensureOpen();
        advanceRehashOnWrite();
        int hash = hash(keyBytes);
        ProbeResult probe = probe(keyBytes, hash);
        if (!probe.found()) {
            return null;
        }
        Location location = probe.location();
        EntryHandle removed = entryHandleAt(table(location.table()), location.slot());
        remove(location, hash);
        return removed;
    }

    public synchronized boolean remove(byte[] keyBytes, EntryHandle expectedHandle) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Objects.requireNonNull(expectedHandle, "expectedHandle");
        ensureOpen();
        advanceRehashOnWrite();
        int hash = hash(keyBytes);
        ProbeResult probe = probe(keyBytes, hash);
        if (!probe.found()) {
            return false;
        }
        Location location = probe.location();
        Table table = table(location.table());
        if (!expectedHandle.nativeHandle().equals(table.entryHandles[location.slot()])) {
            return false;
        }
        remove(location, hash);
        return true;
    }

    public synchronized boolean remove(EntryHandle handle) {
        Objects.requireNonNull(handle, "handle");
        ensureOpen();
        advanceRehashOnWrite();
        Location location = findHandleLocation(handle);
        if (location == null) {
            return false;
        }
        remove(location, topology.hashAt(location));
        return true;
    }

    public synchronized void clear() {
        ensureOpen();
        clearInternal();
    }

    /**
     * 将当前可见目录切换为空目录；退休目录中的 native key 仍由本实例持有，等待 owner thread 后续回收。
     */
    public synchronized void detachEntries() {
        ensureOpen();
        Replacement replacement = emptyReplacement(HashCapacityPolicy.MIN_CAPACITY);
        int currentSize = topology.metrics().size();
        DetachedEntries detached = currentSize == 0 ? null : new DetachedEntries(active, old, currentSize);
        long nextDetachedHeapBytes = detached == null
                ? detachedHeapBytes
                : Math.addExact(detachedHeapBytes, detached.retainedHeapBytes);
        long nextDetachedEntryCount = detached == null
                ? detachedEntryCount
                : Math.addExact(detachedEntryCount, (long) detached.remainingEntries);

        topology.reset(replacement.topology);
        active = replacement.table;
        old = null;
        maintenanceDebt = false;
        refreshMaintenanceRegistration();

        if (detached == null) {
            return;
        }
        if (detachedTail == null) {
            detachedHead = detached;
        } else {
            detachedTail.next = detached;
        }
        detachedTail = detached;
        detachedHeapBytes = nextDetachedHeapBytes;
        detachedEntryCount = nextDetachedEntryCount;
    }

    /**
     * 回收一个已分离 entry。目录槽位会先从退休队列消费，因此回调失败时也不会再次释放同一 native 句柄。
     */
    public synchronized boolean reclaimDetachedEntry(EntryConsumer consumer) {
        Objects.requireNonNull(consumer, "consumer");
        ensureOpen();
        DetachedEntries detached = detachedHead;
        if (detached == null) {
            return false;
        }

        DetachedSlot slot = detached.takeNext();
        detachedEntryCount--;
        Throwable failure = null;
        try {
            consumer.accept(
                    new AllocatorKeyHandle(allocator, slot.keyHandle, hash(slot.keyHandle)),
                    new EntryHandle(slot.entryHandle)
            );
        } catch (RuntimeException | Error next) {
            failure = next;
        }
        try {
            allocator.free(slot.keyHandle);
        } catch (RuntimeException | Error next) {
            failure = addFailure(failure, next);
        }

        if (detached.remainingEntries == 0) {
            detachedHead = detached.next;
            if (detachedHead == null) {
                detachedTail = null;
            }
            detachedHeapBytes -= detached.retainedHeapBytes;
            detached.releaseTables();
        }
        if (failure != null) {
            rethrow(failure);
        }
        return true;
    }

    public synchronized long detachedEntryCount() {
        ensureOpen();
        return detachedEntryCount;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        Throwable failure = null;
        try {
            clearInternal();
        } catch (RuntimeException | Error e) {
            failure = e;
        }
        if (detachedHead != null) {
            failure = addFailure(failure, new IllegalStateException(
                    "native key directory closed with detached entries pending"
            ));
        }
        closed = true;
        if (failure != null) {
            rethrow(failure);
        }
    }

    private void advanceRehashOnWrite() {
        if (topology.metrics().rehashing()) {
            advanceRehash(WRITE_REHASH_BUDGET);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("native key directory is closed");
        }
    }

    private AllocatorKeyHandle randomKeyHandle(Table table, TableSide side) {
        if (table == null) {
            return null;
        }
        int start = ThreadLocalRandom.current().nextInt(table.capacity);
        for (int step = 0; step < table.capacity; step++) {
            int index = (start + step) & (table.capacity - 1);
            if (topology.slotState(side, index) == SlotState.FILLED) {
                return keyHandleAt(new Location(side, index));
            }
        }
        return null;
    }

    private void forEachEntry(Table table, TableSide side, EntryConsumer consumer) {
        if (table == null) {
            return;
        }
        for (int i = 0; i < table.capacity; i++) {
            if (topology.slotState(side, i) == SlotState.FILLED) {
                consumer.accept(keyHandleAt(new Location(side, i)), entryHandleAt(table, i));
            }
        }
    }

    private AllocatorKeyHandle keyHandleAt(Location location) {
        return new AllocatorKeyHandle(
                allocator,
                table(location.table()).keyHandles[location.slot()],
                topology.hashAt(location)
        );
    }

    private static EntryHandle entryHandleAt(Table table, int index) {
        return new EntryHandle(table.entryHandles[index]);
    }

    private boolean acceptScanSlot(Table table, int phase, int index, ScanConsumer consumer) {
        TableSide side = phase == 0 ? TableSide.ACTIVE : TableSide.OLD;
        SlotState state = topology.slotState(side, index);
        if (state == SlotState.FILLED) {
            return consumer.accept(keyHandleAt(new Location(side, index)), entryHandleAt(table, index));
        }
        if (phase != 1 || state != SlotState.MIGRATED_SCAN_SHADOW) {
            return true;
        }
        NativeHandle keyHandle = table.keyHandles[index];
        ProbeResult probe = probeStoredKey(keyHandle, topology.hashAt(new Location(TableSide.OLD, index)));
        return !probe.found()
                || consumer.accept(
                        keyHandleAt(probe.location()),
                        entryHandleAt(table(probe.location().table()), probe.location().slot())
                );
    }

    private ScanCursorV2 normalizeScanCursor(ScanCursorV2 cursor) {
        int currentGeneration = wireGeneration();
        // wire token 只有 generation 的低 29 位；代数不匹配时旧表可能已经退休，只能从当前 active 表重启。
        if (cursor.value() == 0L || cursor.generation() != currentGeneration) {
            return ScanCursorV2.of(currentGeneration, 0, 0L);
        }
        if (cursor.phase() == 1 && !topology.metrics().rehashing()) {
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

    private void clearInternal() {
        Replacement replacement = emptyReplacement(HashCapacityPolicy.MIN_CAPACITY);
        Throwable failure = null;
        try {
            freeEntries(active, TableSide.ACTIVE);
        } catch (RuntimeException | Error e) {
            failure = e;
        }
        try {
            freeEntries(old, TableSide.OLD);
        } catch (RuntimeException | Error e) {
            failure = addFailure(failure, e);
        }
        topology.reset(replacement.topology);
        active = replacement.table;
        old = null;
        maintenanceDebt = false;
        refreshMaintenanceRegistration();
        if (failure != null) {
            rethrow(failure);
        }
    }

    private void freeEntries(Table table, TableSide side) {
        if (table == null) {
            return;
        }
        Throwable failure = null;
        for (int i = 0; i < table.capacity; i++) {
            if (topology.slotState(side, i) != SlotState.FILLED) {
                continue;
            }
            try {
                allocator.free(table.keyHandles[i]);
                table.keyHandles[i] = null;
                table.entryHandles[i] = null;
            } catch (RuntimeException | Error e) {
                failure = addFailure(failure, e);
            }
        }
        if (failure != null) {
            rethrow(failure);
        }
    }

    private Replacement stageTableForInsert(int hash) {
        if (topology.metrics().rehashing()) {
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
        return new Replacement(new Table(capacity), new OpenAddressingTopology(capacity));
    }

    private void publishStagedDirectory(Replacement staged) {
        if (topology.metrics().rehashing()) {
            throw new IllegalStateException("cannot start a second key-directory rehash");
        }
        Replacement replacement = Objects.requireNonNull(staged, "staged");
        Table previous = active;
        topology.beginRehash(replacement.topology);
        old = previous;
        active = replacement.table;
        maintenanceDebt = true;
        refreshMaintenanceRegistration();
    }

    private void insertActive(NativeHandle keyHandle, EntryHandle entryHandle, int hash) {
        ProbeResult probe = topology.probe(hash, ignored -> false);
        if (probe.found()) {
            throw new IllegalStateException("native key appeared during insert");
        }
        int index = probe.location().slot();
        Objects.requireNonNull(entryHandle, "entryHandle");
        active.keyHandles[index] = Objects.requireNonNull(keyHandle, "keyHandle");
        active.entryHandles[index] = entryHandle.nativeHandle();
        topology.occupyActive(index, hash);
    }

    private void remove(Location location, int hash) {
        Table table = table(location.table());
        int index = location.slot();
        NativeHandle keyHandle = table.keyHandles[index];
        if (location.table() == TableSide.ACTIVE) {
            invalidateOldShadow(keyHandle, hash);
        }
        allocator.free(keyHandle);
        table.keyHandles[index] = null;
        table.entryHandles[index] = null;
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
        if (topology.metrics().rehashing()) {
            maintenanceDebt = true;
        } else {
            maintenanceDebt = maintenanceDecision().action() != HashCapacityPolicy.Action.NONE;
        }
        refreshMaintenanceRegistration();
    }

    private void refreshMaintenanceRegistration() {
        if (maintenanceRegistration == null) {
            return;
        }
        if (maintenanceDebt || topology.metrics().rehashing()) {
            if (!maintenanceRegistration.registered()) {
                maintenanceRegistry.register(maintenanceRegistration);
            }
        } else {
            maintenanceRegistry.unregister(maintenanceRegistration);
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
            old.entryHandles[index] = null;
        }
    }

    private ProbeResult probe(byte[] keyBytes, int hash) {
        return topology.probe(
                hash,
                location -> equalsBytes(table(location.table()).keyHandles[location.slot()], keyBytes)
        );
    }

    private ProbeResult probeStoredKey(NativeHandle keyHandle, int hash) {
        return topology.probe(
                hash,
                location -> keyHandle.equals(table(location.table()).keyHandles[location.slot()])
        );
    }

    private Location findHandleLocation(EntryHandle handle) {
        for (TableSide side : TableSide.values()) {
            Table table = table(side);
            if (table == null) {
                continue;
            }
            for (int slot = 0; slot < table.capacity; slot++) {
                if (topology.slotState(side, slot) == SlotState.FILLED
                        && handle.nativeHandle().equals(table.entryHandles[slot])) {
                    return new Location(side, slot);
                }
            }
        }
        return null;
    }

    private Table table(TableSide side) {
        return side == TableSide.ACTIVE ? active : old;
    }

    private int hash(byte[] keyBytes) {
        return SipHash24.foldToInt(SipHash24.hash(hashSeed, keyBytes));
    }

    private int hash(NativeHandle keyHandle) {
        try (NativeObjectView view = allocator.resolve(keyHandle, NativeAccessMode.READ_ONLY)) {
            return SipHash24.foldToInt(SipHash24.hash(hashSeed, view));
        }
    }

    private NativeHandle allocateKey(byte[] keyBytes) {
        NativeHandle handle = allocator.allocate(NativeObjectKind.KEY_BYTES, keyBytes.length);
        boolean initialized = false;
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
            if (keyBytes.length > 0) {
                view.setBytes(0, keyBytes, 0, keyBytes.length);
            }
            initialized = true;
            return handle;
        } finally {
            if (!initialized) {
                allocator.free(handle);
            }
        }
    }

    private boolean equalsBytes(NativeHandle handle, byte[] keyBytes) {
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
            return view.size() == keyBytes.length
                    && view.contentEquals(0, keyBytes, 0, keyBytes.length);
        }
    }

    private static Throwable addFailure(Throwable failure, Throwable next) {
        if (failure == null) {
            return next;
        }
        failure.addSuppressed(next);
        return failure;
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException e) {
            throw e;
        }
        if (failure instanceof Error e) {
            throw e;
        }
        throw new IllegalStateException(failure);
    }

    private static long payloadHeapBytesForCapacity(int capacity) {
        long keyHandleArray = ARRAY_HEADER_BYTES + (long) capacity * REFERENCE_BYTES;
        long entryHandleArray = ARRAY_HEADER_BYTES + (long) capacity * REFERENCE_BYTES;
        return TABLE_OBJECT_BYTES + keyHandleArray + entryHandleArray;
    }

    private static long committedHeapBytesForCapacity(int capacity) {
        return payloadHeapBytesForCapacity(capacity)
                + OpenAddressingTopology.standaloneHeapEstimatedBytes(capacity);
    }

    private static long stagedReplacementHeapBytesForCapacity(int capacity) {
        return REPLACEMENT_OBJECT_BYTES + committedHeapBytesForCapacity(capacity);
    }

    @FunctionalInterface
    public interface EntryConsumer {
        void accept(AllocatorKeyHandle keyHandle, EntryHandle entryHandle);
    }

    public record ScanResult(
            ScanCursorV2 startCursor,
            ScanCursorV2 nextCursor,
            long inspectedSlots,
            long tableGeneration
    ) {
    }

    @FunctionalInterface
    public interface ScanConsumer {
        boolean accept(AllocatorKeyHandle keyHandle, EntryHandle entryHandle);
    }

    private static final class DetachedEntries {
        private final long retainedHeapBytes;
        private Table first;
        private Table second;
        private int tableIndex;
        private int slotIndex;
        private int remainingEntries;
        private DetachedEntries next;

        private DetachedEntries(Table first, Table second, int remainingEntries) {
            this.first = Objects.requireNonNull(first, "first");
            this.second = second;
            this.remainingEntries = remainingEntries;
            int tableEntries = liveEntryCount(first) + liveEntryCount(second);
            if (remainingEntries <= 0 || tableEntries != remainingEntries) {
                throw new IllegalStateException(
                        "detached key-directory size mismatch: expected=" + remainingEntries
                                + ", tables=" + tableEntries
                );
            }
            this.retainedHeapBytes = Math.addExact(
                    DETACHED_ENTRIES_OBJECT_BYTES,
                    Math.addExact(first.heapBytes, second == null ? 0L : second.heapBytes)
            );
        }

        private DetachedSlot takeNext() {
            while (tableIndex < 2) {
                Table table = tableIndex == 0 ? first : second;
                if (table == null || slotIndex >= table.capacity) {
                    tableIndex++;
                    slotIndex = 0;
                    continue;
                }
                int index = slotIndex++;
                // migrated shadow 仍保留 key，但 entry 已移到 active；非空 entry 是 detached ownership marker。
                if (table.entryHandles[index] == null) {
                    continue;
                }
                DetachedSlot slot = new DetachedSlot(
                        table.keyHandles[index],
                        table.entryHandles[index]
                );
                table.keyHandles[index] = null;
                table.entryHandles[index] = null;
                remainingEntries--;
                return slot;
            }
            throw new IllegalStateException("detached key-directory entry count exceeds live slots");
        }

        private void releaseTables() {
            first = null;
            second = null;
            next = null;
        }

        private static int liveEntryCount(Table table) {
            if (table == null) {
                return 0;
            }
            int count = 0;
            for (NativeHandle entryHandle : table.entryHandles) {
                if (entryHandle != null) {
                    count++;
                }
            }
            return count;
        }
    }

    private record DetachedSlot(NativeHandle keyHandle, NativeHandle entryHandle) {
        private DetachedSlot {
            Objects.requireNonNull(keyHandle, "keyHandle");
            Objects.requireNonNull(entryHandle, "entryHandle");
        }
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
                throw new IllegalStateException("staged key-directory resize is closed");
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

    public final class StagedInsert implements AutoCloseable {
        private final byte[] keyBytes;
        private final int hash;
        private final Replacement replacement;
        private NativeHandle keyHandle;
        private boolean terminal;

        private StagedInsert(byte[] keyBytes, int hash, NativeHandle keyHandle, Replacement replacement) {
            this.keyBytes = keyBytes;
            this.hash = hash;
            this.keyHandle = Objects.requireNonNull(keyHandle, "keyHandle");
            if (keyHandle.isNull()) {
                throw new IllegalArgumentException("keyHandle must not be null");
            }
            this.replacement = replacement;
        }

        public AllocatorKeyHandle keyHandle() {
            ensureActive();
            return new AllocatorKeyHandle(allocator, keyHandle, hash);
        }

        public long stagedHeapBytes() {
            return replacement == null ? 0L : replacement.stagedHeapBytes();
        }

        private NativeHandle keyHandleForPublish() {
            ensureActive();
            return keyHandle;
        }

        private void markPublished() {
            ensureActive();
            keyHandle = null;
            terminal = true;
        }

        private void ensureActive() {
            if (terminal || keyHandle == null) {
                throw new IllegalStateException("staged native key insert is closed");
            }
        }

        @Override
        public void close() {
            if (terminal) {
                return;
            }
            terminal = true;
            NativeHandle handle = keyHandle;
            keyHandle = null;
            if (handle != null) {
                allocator.free(handle);
            }
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

    private static final class Table {
        private final int capacity;
        private final NativeHandle[] keyHandles;
        private final NativeHandle[] entryHandles;
        private final long heapBytes;

        private Table(int capacity) {
            if (capacity < HashCapacityPolicy.MIN_CAPACITY
                    || capacity > HashCapacityPolicy.MAX_CAPACITY
                    || (capacity & (capacity - 1)) != 0) {
                throw new IllegalArgumentException("invalid key-directory capacity: " + capacity);
            }
            this.capacity = capacity;
            this.keyHandles = new NativeHandle[capacity];
            this.entryHandles = new NativeHandle[capacity];
            this.heapBytes = payloadHeapBytesForCapacity(capacity);
        }
    }
}
