package yier.bubu.redis.storage.memory.internal.keyspace;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
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
import yier.bubu.redis.storage.memory.internal.hash.SipHash24;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;

public final class NativeKeyDirectory implements AutoCloseable, HashTableMaintenanceRegistry.Participant {
    private static final byte STATE_EMPTY = 0;
    private static final byte STATE_FILLED = 1;
    private static final byte STATE_TOMBSTONE = 2;
    private static final byte STATE_MIGRATED_SCAN_SHADOW = 3;
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long HANDLE_BYTES = Long.BYTES * 2L;
    private static final long TABLE_OBJECT_BYTES = 48L;
    private static final long DETACHED_ENTRIES_OBJECT_BYTES = 64L;
    private static final HashTableWorkBudget WRITE_REHASH_BUDGET = HashTableWorkBudget.of(2L, Long.MAX_VALUE);

    private final StableMemoryBackend allocator;
    private final HashSeed hashSeed;
    private final HashTableMaintenanceRegistry maintenanceRegistry;
    private final HashTableMaintenanceRegistry.Registration maintenanceRegistration;
    private Table active;
    private Table old;
    private int rehashCursor;
    private int size;
    private long generation;
    private long completedRehashes;
    private int maximumProbeLength;
    private boolean maintenanceDebt;
    private DetachedEntries detachedHead;
    private DetachedEntries detachedTail;
    private long detachedHeapBytes;
    private long detachedEntryCount;
    private boolean closed;
    private boolean iterationTrapForTesting;

    public NativeKeyDirectory(
            StableMemoryBackend allocator,
            HashSeed hashSeed,
            HashTableMaintenanceRegistry maintenanceRegistry
    ) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.hashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
        this.maintenanceRegistry = maintenanceRegistry;
        this.maintenanceRegistration = maintenanceRegistry == null ? null : maintenanceRegistry.registration(this);
        this.active = new Table(HashCapacityPolicy.MIN_CAPACITY);
    }

    public synchronized int size() {
        return size;
    }

    public synchronized long nativeBytes() {
        ensureOpen();
        return 0L;
    }

    public synchronized long heapBytes() {
        ensureOpen();
        return active.heapBytes + (old == null ? 0L : old.heapBytes) + detachedHeapBytes;
    }

    public synchronized long estimatedInsertHeapGrowthBytes() {
        ensureOpen();
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

    public synchronized HashTableMetrics metrics() {
        ensureOpen();
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

    public synchronized long tableGeneration() {
        ensureOpen();
        return generation;
    }

    @Override
    public synchronized boolean hasMaintenanceDebt() {
        ensureOpen();
        return maintenanceDebt || old != null;
    }

    public synchronized StagedResize stageMaintenanceResize() {
        ensureOpen();
        if (old != null) {
            return null;
        }
        HashCapacityPolicy.Decision decision = maintenanceDecision();
        if (decision.action() == HashCapacityPolicy.Action.NONE) {
            maintenanceDebt = false;
            refreshMaintenanceRegistration();
            return null;
        }
        return new StagedResize(active, new Table(decision.targetCapacity()));
    }

    @Override
    public synchronized long estimatedMaintenanceGrowthBytes() {
        ensureOpen();
        if (old != null) {
            return 0L;
        }
        HashCapacityPolicy.Decision decision = maintenanceDecision();
        return decision.action() == HashCapacityPolicy.Action.NONE
                ? 0L
                : heapBytesForCapacity(decision.targetCapacity());
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
        if (old != null || active != staged.source) {
            throw new IllegalStateException("staged key-directory resize is no longer current");
        }
        publishStagedDirectory(staged.publish());
    }

    @Override
    public synchronized HashTableWorkResult advanceRehash(HashTableWorkBudget budget) {
        Objects.requireNonNull(budget, "budget");
        ensureOpen();
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
        return new HashTableWorkResult(inspected, migrated, true, HashTableWorkResult.StopReason.COMPLETE);
    }

    public synchronized EntryHandle get(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        ensureOpen();
        int hash = hash(keyBytes);
        int index = findIndex(active, keyBytes, hash);
        if (index >= 0) {
            return entryHandleAt(active, index);
        }
        index = findIndex(old, keyBytes, hash);
        return index < 0 ? null : entryHandleAt(old, index);
    }

    public synchronized KeyHandle getKeyHandle(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        ensureOpen();
        int hash = hash(keyBytes);
        int index = findIndex(active, keyBytes, hash);
        if (index >= 0) {
            return keyHandleAt(active, index);
        }
        index = findIndex(old, keyBytes, hash);
        return index < 0 ? null : keyHandleAt(old, index);
    }

    public synchronized KeyHandle randomKeyHandle() {
        ensureOpen();
        if (size == 0) {
            return null;
        }
        KeyHandle fromActive = randomKeyHandle(active);
        return fromActive != null ? fromActive : randomKeyHandle(old);
    }

    public synchronized void forEachEntry(EntryConsumer consumer) {
        Objects.requireNonNull(consumer, "consumer");
        ensureOpen();
        if (iterationTrapForTesting) {
            throw new AssertionError("key-directory iteration is forbidden while taking a memory snapshot");
        }
        forEachEntry(active, consumer);
        forEachEntry(old, consumer);
    }

    public synchronized void armIterationTrapForTesting() {
        iterationTrapForTesting = true;
    }

    public synchronized void disarmIterationTrapForTesting() {
        iterationTrapForTesting = false;
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
            byte state = table.states[index];
            boolean visible = state == STATE_FILLED || (phase == 1 && state == STATE_MIGRATED_SCAN_SHADOW);
            if (visible && !consumer.accept(keyHandleAt(table, index), entryHandleAt(table, index))) {
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

    public synchronized EntryHandle compute(
            byte[] keyBytes,
            BiFunction<? super byte[], ? super EntryHandle, ? extends EntryHandle> remappingFunction
    ) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Objects.requireNonNull(remappingFunction, "remappingFunction");
        ensureOpen();
        advanceRehashOnWrite();

        int hash = hash(keyBytes);
        int activeIndex = findIndex(active, keyBytes, hash);
        if (activeIndex >= 0) {
            EntryHandle oldHandle = entryHandleAt(active, activeIndex);
            EntryHandle newHandle = remappingFunction.apply(keyBytes, oldHandle);
            if (newHandle == null) {
                removeFromActive(activeIndex);
                return null;
            }
            active.entryHandles[activeIndex] = newHandle.nativeHandle();
            return newHandle;
        }

        int oldIndex = findIndex(old, keyBytes, hash);
        if (oldIndex >= 0) {
            EntryHandle oldHandle = entryHandleAt(old, oldIndex);
            EntryHandle newHandle = remappingFunction.apply(keyBytes, oldHandle);
            if (newHandle == null) {
                removeFromOld(oldIndex);
                return null;
            }
            moveOldSlotToActive(oldIndex);
            int movedIndex = findIndex(active, keyBytes, hash);
            if (movedIndex < 0) {
                throw new IllegalStateException("migrated native key is missing from active table");
            }
            active.entryHandles[movedIndex] = newHandle.nativeHandle();
            return newHandle;
        }

        Table staged = stageDirectoryForInsert(keyBytes, hash);
        EntryHandle newHandle = remappingFunction.apply(keyBytes, null);
        if (newHandle == null) {
            return null;
        }
        if (staged != null) {
            publishStagedDirectory(staged);
        }
        insertActive(allocateKey(keyBytes), newHandle, hash, keyBytes);
        recordMaintenanceDebt();
        return newHandle;
    }

    public StagedInsert stageInsert(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        byte[] stableKeyBytes = Arrays.copyOf(keyBytes, keyBytes.length);
        synchronized (this) {
            ensureOpen();
            advanceRehashOnWrite();
            int hash = hash(stableKeyBytes);
            if (findIndex(active, stableKeyBytes, hash) >= 0 || findIndex(old, stableKeyBytes, hash) >= 0) {
                throw new IllegalStateException("native key already exists during staged insert");
            }
            Table staged = stageDirectoryForInsert(stableKeyBytes, hash);
            NativeHandle keyHandle = allocateKey(stableKeyBytes);
            return new StagedInsert(stableKeyBytes, hash, keyHandle, staged);
        }
    }

    public synchronized void publishStagedInsert(StagedInsert staged, EntryHandle entryHandle) {
        Objects.requireNonNull(staged, "staged");
        Objects.requireNonNull(entryHandle, "entryHandle");
        ensureOpen();
        staged.ensureActive();
        if (findIndex(active, staged.keyBytes, staged.hash) >= 0 || findIndex(old, staged.keyBytes, staged.hash) >= 0) {
            throw new IllegalStateException("native key appeared during staged insert");
        }
        if (staged.directory != null) {
            publishStagedDirectory(staged.directory);
        }
        recordMaintenanceDebt();
        NativeHandle keyHandle = staged.keyHandleForPublish();
        insertActive(keyHandle, entryHandle, staged.hash, staged.keyBytes);
        staged.markPublished();
    }

    public synchronized EntryHandle remove(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        ensureOpen();
        advanceRehashOnWrite();
        int hash = hash(keyBytes);
        int index = findIndex(active, keyBytes, hash);
        if (index >= 0) {
            EntryHandle removed = entryHandleAt(active, index);
            removeFromActive(index);
            return removed;
        }
        index = findIndex(old, keyBytes, hash);
        if (index < 0) {
            return null;
        }
        EntryHandle removed = entryHandleAt(old, index);
        removeFromOld(index);
        return removed;
    }

    public synchronized boolean remove(byte[] keyBytes, EntryHandle expectedHandle) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Objects.requireNonNull(expectedHandle, "expectedHandle");
        ensureOpen();
        advanceRehashOnWrite();
        int hash = hash(keyBytes);
        int index = findIndex(active, keyBytes, hash);
        if (index >= 0) {
            if (!expectedHandle.nativeHandle().equals(active.entryHandles[index])) {
                return false;
            }
            removeFromActive(index);
            return true;
        }
        index = findIndex(old, keyBytes, hash);
        if (index < 0 || !expectedHandle.nativeHandle().equals(old.entryHandles[index])) {
            return false;
        }
        removeFromOld(index);
        return true;
    }

    public synchronized boolean remove(EntryHandle handle) {
        Objects.requireNonNull(handle, "handle");
        ensureOpen();
        advanceRehashOnWrite();
        int index = findHandleIndex(active, handle);
        if (index >= 0) {
            removeFromActive(index);
            return true;
        }
        index = findHandleIndex(old, handle);
        if (index < 0) {
            return false;
        }
        removeFromOld(index);
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
        Table replacement = new Table(HashCapacityPolicy.MIN_CAPACITY);
        DetachedEntries detached = size == 0 ? null : new DetachedEntries(active, old, size);
        long nextDetachedHeapBytes = detached == null
                ? detachedHeapBytes
                : Math.addExact(detachedHeapBytes, detached.retainedHeapBytes);
        long nextDetachedEntryCount = detached == null
                ? detachedEntryCount
                : Math.addExact(detachedEntryCount, (long) detached.remainingEntries);

        active = replacement;
        old = null;
        rehashCursor = 0;
        size = 0;
        maintenanceDebt = false;
        refreshMaintenanceRegistration();
        generation++;

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
                    KeyHandle.forNative(allocator, slot.keyHandle, slot.hash),
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
        if (old != null) {
            advanceRehash(WRITE_REHASH_BUDGET);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("native key directory is closed");
        }
    }

    private KeyHandle randomKeyHandle(Table table) {
        if (table == null || table.size == 0) {
            return null;
        }
        int start = ThreadLocalRandom.current().nextInt(table.capacity);
        for (int step = 0; step < table.capacity; step++) {
            int index = (start + step) & (table.capacity - 1);
            if (table.states[index] == STATE_FILLED) {
                return keyHandleAt(table, index);
            }
        }
        return null;
    }

    private void forEachEntry(Table table, EntryConsumer consumer) {
        if (table == null) {
            return;
        }
        for (int i = 0; i < table.capacity; i++) {
            if (table.states[i] == STATE_FILLED) {
                consumer.accept(keyHandleAt(table, i), entryHandleAt(table, i));
            }
        }
    }

    private KeyHandle keyHandleAt(Table table, int index) {
        return KeyHandle.forNative(allocator, table.keyHandles[index], table.hashes[index]);
    }

    private static EntryHandle entryHandleAt(Table table, int index) {
        return new EntryHandle(table.entryHandles[index]);
    }

    private ScanCursorV2 normalizeScanCursor(ScanCursorV2 cursor) {
        int currentGeneration = wireGeneration();
        // wire token 只有 generation 的低 29 位；代数不匹配时旧表可能已经退休，只能从当前 active 表重启。
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

    private void clearInternal() {
        Throwable failure = null;
        try {
            freeEntries(active);
        } catch (RuntimeException | Error e) {
            failure = e;
        }
        try {
            freeEntries(old);
        } catch (RuntimeException | Error e) {
            failure = addFailure(failure, e);
        }
        active = new Table(HashCapacityPolicy.MIN_CAPACITY);
        old = null;
        rehashCursor = 0;
        size = 0;
        maintenanceDebt = false;
        refreshMaintenanceRegistration();
        generation++;
        if (failure != null) {
            rethrow(failure);
        }
    }

    private void freeEntries(Table table) {
        if (table == null) {
            return;
        }
        Throwable failure = null;
        for (int i = 0; i < table.capacity; i++) {
            if (table.states[i] != STATE_FILLED) {
                continue;
            }
            try {
                allocator.free(table.keyHandles[i]);
                table.keyHandles[i] = null;
                table.entryHandles[i] = null;
                table.hashes[i] = 0;
                table.states[i] = STATE_EMPTY;
            } catch (RuntimeException | Error e) {
                failure = addFailure(failure, e);
            }
        }
        if (failure != null) {
            rethrow(failure);
        }
    }

    private Table stageDirectoryForInsert(byte[] keyBytes, int hash) {
        if (old != null) {
            return null;
        }
        int insertIndex = findInsertIndex(active, keyBytes, hash);
        byte previousState = active.states[insertIndex];
        if (previousState == STATE_FILLED) {
            throw new IllegalStateException("native key already exists during staged insert");
        }
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

    private void publishStagedDirectory(Table staged) {
        if (old != null) {
            throw new IllegalStateException("cannot start a second key-directory rehash");
        }
        old = active;
        active = Objects.requireNonNull(staged, "staged");
        rehashCursor = 0;
        maintenanceDebt = true;
        refreshMaintenanceRegistration();
        generation++;
    }

    private void insertActive(NativeHandle keyHandle, EntryHandle entryHandle, int hash, byte[] keyBytes) {
        int index = findInsertIndex(active, keyBytes, hash);
        if (active.states[index] == STATE_FILLED) {
            throw new IllegalStateException("native key appeared during insert");
        }
        Objects.requireNonNull(entryHandle, "entryHandle");
        writeFilled(active, index, keyHandle, entryHandle.nativeHandle(), hash);
        size++;
    }

    private void moveOldSlotToActive(int oldIndex) {
        if (old == null || old.states[oldIndex] != STATE_FILLED) {
            return;
        }
        NativeHandle keyHandle = old.keyHandles[oldIndex];
        NativeHandle entryHandle = old.entryHandles[oldIndex];
        int hash = old.hashes[oldIndex];
        int activeIndex = findInsertionIndex(active, hash);
        if (active.states[activeIndex] == STATE_FILLED) {
            throw new IllegalStateException("duplicate native key while rehashing key directory");
        }
        writeFilled(active, activeIndex, keyHandle, entryHandle, hash);
        old.states[oldIndex] = STATE_MIGRATED_SCAN_SHADOW;
        old.size--;
        old.filled--;
    }

    private void writeFilled(
            Table table,
            int index,
            NativeHandle keyHandle,
            NativeHandle entryHandle,
            int hash
    ) {
        byte previous = table.states[index];
        if (previous == STATE_EMPTY) {
            table.filled++;
        } else if (previous == STATE_TOMBSTONE) {
            table.tombstones--;
        } else {
            throw new IllegalStateException("attempted to overwrite a live hash-table slot");
        }
        table.keyHandles[index] = Objects.requireNonNull(keyHandle, "keyHandle");
        table.entryHandles[index] = Objects.requireNonNull(entryHandle, "entryHandle");
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
        allocator.free(table.keyHandles[index]);
        table.keyHandles[index] = null;
        table.entryHandles[index] = null;
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
                old.entryHandles[index] = null;
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
                    && equalsBytes(table.keyHandles[index], keyBytes)) {
                return index;
            }
            index = (index + 1) & mask;
        }
        return -1;
    }

    private int findHandleIndex(Table table, EntryHandle handle) {
        if (table == null) {
            return -1;
        }
        for (int i = 0; i < table.capacity; i++) {
            if (table.states[i] == STATE_FILLED
                    && handle.nativeHandle().equals(table.entryHandles[i])) {
                return i;
            }
        }
        return -1;
    }

    private int findInsertIndex(Table table, byte[] keyBytes, int hash) {
        int mask = table.capacity - 1;
        int index = hash & mask;
        int firstTombstone = -1;
        for (int probes = 1; probes <= table.capacity; probes++) {
            recordProbe(probes);
            byte state = table.states[index];
            if (state == STATE_EMPTY) {
                return firstTombstone >= 0 ? firstTombstone : index;
            }
            if (state == STATE_TOMBSTONE) {
                if (firstTombstone < 0) {
                    firstTombstone = index;
                }
            } else if (state == STATE_FILLED
                    && table.hashes[index] == hash
                    && equalsBytes(table.keyHandles[index], keyBytes)) {
                return index;
            }
            index = (index + 1) & mask;
        }
        if (firstTombstone >= 0) {
            return firstTombstone;
        }
        throw new IllegalStateException("key directory has no insertable slot");
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
        throw new IllegalStateException("active key directory has no insertion slot");
    }

    private void recordProbe(int probes) {
        if (probes > maximumProbeLength) {
            maximumProbeLength = probes;
        }
    }

    private int hash(byte[] keyBytes) {
        return SipHash24.foldToInt(SipHash24.hash(hashSeed, keyBytes));
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

    private static boolean timeLimitReached(long startedAt, long timeLimitNanos) {
        return timeLimitNanos != Long.MAX_VALUE && System.nanoTime() - startedAt >= timeLimitNanos;
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

    private static long heapBytesForCapacity(int capacity) {
        long keyHandleArray = ARRAY_HEADER_BYTES + (long) capacity * HANDLE_BYTES;
        long entryHandleArray = ARRAY_HEADER_BYTES + (long) capacity * HANDLE_BYTES;
        long hashArray = ARRAY_HEADER_BYTES + (long) capacity * Integer.BYTES;
        long stateArray = ARRAY_HEADER_BYTES + capacity;
        return TABLE_OBJECT_BYTES + keyHandleArray + entryHandleArray + hashArray + stateArray;
    }

    @FunctionalInterface
    public interface EntryConsumer {
        void accept(KeyHandle keyHandle, EntryHandle entryHandle);
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
        boolean accept(KeyHandle keyHandle, EntryHandle entryHandle);
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
            int tableEntries = first.size + (second == null ? 0 : second.size);
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
                if (table.states[index] != STATE_FILLED) {
                    continue;
                }
                DetachedSlot slot = new DetachedSlot(
                        table.keyHandles[index],
                        table.entryHandles[index],
                        table.hashes[index]
                );
                table.keyHandles[index] = null;
                table.entryHandles[index] = null;
                table.hashes[index] = 0;
                table.states[index] = STATE_EMPTY;
                table.size--;
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
    }

    private record DetachedSlot(NativeHandle keyHandle, NativeHandle entryHandle, int hash) {
        private DetachedSlot {
            Objects.requireNonNull(keyHandle, "keyHandle");
            Objects.requireNonNull(entryHandle, "entryHandle");
        }
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
        private final Table directory;
        private NativeHandle keyHandle;
        private boolean terminal;

        private StagedInsert(byte[] keyBytes, int hash, NativeHandle keyHandle, Table directory) {
            this.keyBytes = keyBytes;
            this.hash = hash;
            this.keyHandle = Objects.requireNonNull(keyHandle, "keyHandle");
            if (keyHandle.isNull()) {
                throw new IllegalArgumentException("keyHandle must not be null");
            }
            this.directory = directory;
        }

        public KeyHandle keyHandle() {
            ensureActive();
            return KeyHandle.forNative(allocator, keyHandle, hash);
        }

        public long stagedHeapBytes() {
            return directory == null ? 0L : directory.heapBytes;
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

    private static final class Table {
        private final int capacity;
        private final NativeHandle[] keyHandles;
        private final NativeHandle[] entryHandles;
        private final int[] hashes;
        private final byte[] states;
        private final long heapBytes;
        private int size;
        private int filled;
        private int tombstones;

        private Table(int capacity) {
            if (capacity < HashCapacityPolicy.MIN_CAPACITY
                    || capacity > HashCapacityPolicy.MAX_CAPACITY
                    || (capacity & (capacity - 1)) != 0) {
                throw new IllegalArgumentException("invalid key-directory capacity: " + capacity);
            }
            this.capacity = capacity;
            this.keyHandles = new NativeHandle[capacity];
            this.entryHandles = new NativeHandle[capacity];
            this.hashes = new int[capacity];
            this.states = new byte[capacity];
            this.heapBytes = heapBytesForCapacity(capacity);
        }
    }
}
