package yier.bubu.redis.storage.memory.internal.keyspace;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.hash.HashCapacityPolicy;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMetrics;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkBudget;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkResult;
import yier.bubu.redis.storage.memory.internal.hash.SipHash24;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;

public final class NativeKeyDirectory implements AutoCloseable {
    private static final byte STATE_EMPTY = 0;
    private static final byte STATE_FILLED = 1;
    private static final byte STATE_TOMBSTONE = 2;
    private static final byte STATE_MIGRATED_SCAN_SHADOW = 3;
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long REFERENCE_BYTES = 8L;
    private static final long TABLE_OBJECT_BYTES = 48L;
    private static final HashTableWorkBudget WRITE_REHASH_BUDGET = HashTableWorkBudget.of(2L, Long.MAX_VALUE);

    private final NativeAllocator allocator;
    private final boolean ownsAllocator;
    private final HashSeed hashSeed;
    private Table active;
    private Table old;
    private int rehashCursor;
    private int size;
    private long generation;
    private long completedRehashes;
    private int maximumProbeLength;
    private boolean maintenanceDebt;
    private boolean closed;

    public NativeKeyDirectory(YierdisFfmMemoryRuntime runtime) {
        this(runtime, HashSeed.random());
    }

    public NativeKeyDirectory(YierdisFfmMemoryRuntime runtime, HashSeed hashSeed) {
        this(new YierdisStableNativeAllocator(Objects.requireNonNull(runtime, "runtime"), 4096), true, hashSeed);
    }

    public NativeKeyDirectory(NativeAllocator allocator) {
        this(allocator, false, HashSeed.random());
    }

    public NativeKeyDirectory(NativeAllocator allocator, HashSeed hashSeed) {
        this(allocator, false, hashSeed);
    }

    private NativeKeyDirectory(NativeAllocator allocator, boolean ownsAllocator, HashSeed hashSeed) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.ownsAllocator = ownsAllocator;
        this.hashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
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
        return active.heapBytes + (old == null ? 0L : old.heapBytes);
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
            return null;
        }
        return new StagedResize(active, new Table(decision.targetCapacity()));
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
            return active.handles[index];
        }
        index = findIndex(old, keyBytes, hash);
        return index < 0 ? null : old.handles[index];
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
        forEachEntry(active, consumer);
        forEachEntry(old, consumer);
    }

    public synchronized ScanCursorV2 scan(ScanCursorV2 cursor, int maxSteps, ScanConsumer consumer) {
        Objects.requireNonNull(cursor, "cursor");
        Objects.requireNonNull(consumer, "consumer");
        ensureOpen();
        if (maxSteps <= 0 || size == 0) {
            return ScanCursorV2.start();
        }

        long totalSlots = active.capacity + (long) (old == null ? 0 : old.capacity);
        long position = Math.min(cursor.position(), totalSlots);
        int inspected = 0;
        while (position < totalSlots && inspected < maxSteps) {
            Table table = position < active.capacity ? active : old;
            int index = (int) (position < active.capacity ? position : position - active.capacity);
            inspected++;
            if (table.states[index] == STATE_FILLED
                    && !consumer.accept(keyHandleAt(table, index), table.handles[index])) {
                position++;
                return nextLegacyCursor(position, totalSlots);
            }
            position++;
        }
        return nextLegacyCursor(position, totalSlots);
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
            EntryHandle oldHandle = active.handles[activeIndex];
            EntryHandle newHandle = remappingFunction.apply(keyBytes, oldHandle);
            if (newHandle == null) {
                removeFromActive(activeIndex);
                return null;
            }
            active.handles[activeIndex] = newHandle;
            return newHandle;
        }

        int oldIndex = findIndex(old, keyBytes, hash);
        if (oldIndex >= 0) {
            EntryHandle oldHandle = old.handles[oldIndex];
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
            active.handles[movedIndex] = newHandle;
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
        insertActive(staged.publish(), entryHandle, staged.hash, staged.keyBytes);
        recordMaintenanceDebt();
    }

    public synchronized EntryHandle remove(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        ensureOpen();
        advanceRehashOnWrite();
        int hash = hash(keyBytes);
        int index = findIndex(active, keyBytes, hash);
        if (index >= 0) {
            EntryHandle removed = active.handles[index];
            removeFromActive(index);
            return removed;
        }
        index = findIndex(old, keyBytes, hash);
        if (index < 0) {
            return null;
        }
        EntryHandle removed = old.handles[index];
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
            if (!expectedHandle.equals(active.handles[index])) {
                return false;
            }
            removeFromActive(index);
            return true;
        }
        index = findIndex(old, keyBytes, hash);
        if (index < 0 || !expectedHandle.equals(old.handles[index])) {
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
        if (ownsAllocator) {
            try {
                allocator.close();
            } catch (RuntimeException | Error e) {
                failure = addFailure(failure, e);
            }
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
                consumer.accept(keyHandleAt(table, i), table.handles[i]);
            }
        }
    }

    private KeyHandle keyHandleAt(Table table, int index) {
        return KeyHandle.forNative(allocator, table.keyHandles[index], table.hashes[index]);
    }

    private ScanCursorV2 nextLegacyCursor(long nextPosition, long totalSlots) {
        return nextPosition >= totalSlots ? ScanCursorV2.start() : ScanCursorV2.of(nextPosition);
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
                table.handles[i] = null;
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
        generation++;
    }

    private void insertActive(NativeHandle keyHandle, EntryHandle entryHandle, int hash, byte[] keyBytes) {
        int index = findInsertIndex(active, keyBytes, hash);
        if (active.states[index] == STATE_FILLED) {
            throw new IllegalStateException("native key appeared during insert");
        }
        writeFilled(active, index, keyHandle, entryHandle, hash);
        size++;
    }

    private void moveOldSlotToActive(int oldIndex) {
        if (old == null || old.states[oldIndex] != STATE_FILLED) {
            return;
        }
        NativeHandle keyHandle = old.keyHandles[oldIndex];
        EntryHandle entryHandle = old.handles[oldIndex];
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

    private void writeFilled(Table table, int index, NativeHandle keyHandle, EntryHandle entryHandle, int hash) {
        byte previous = table.states[index];
        if (previous == STATE_EMPTY) {
            table.filled++;
        } else if (previous == STATE_TOMBSTONE) {
            table.tombstones--;
        } else {
            throw new IllegalStateException("attempted to overwrite a live hash-table slot");
        }
        table.keyHandles[index] = Objects.requireNonNull(keyHandle, "keyHandle");
        table.handles[index] = Objects.requireNonNull(entryHandle, "entryHandle");
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
        table.handles[index] = null;
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
            return;
        }
        maintenanceDebt = maintenanceDecision().action() != HashCapacityPolicy.Action.NONE;
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
                old.handles[index] = null;
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
            if (state == STATE_FILLED && table.hashes[index] == hash && equalsBytes(table.keyHandles[index], keyBytes)) {
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
            if (table.states[i] == STATE_FILLED && handle.equals(table.handles[i])) {
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
            } else if (state == STATE_FILLED && table.hashes[index] == hash && equalsBytes(table.keyHandles[index], keyBytes)) {
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
            if (view.size() != keyBytes.length) {
                return false;
            }
            for (int i = 0; i < keyBytes.length; i++) {
                if (view.getByte(i) != keyBytes[i]) {
                    return false;
                }
            }
            return true;
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
        long keyHandleArray = ARRAY_HEADER_BYTES + (long) capacity * REFERENCE_BYTES;
        long entryHandleArray = ARRAY_HEADER_BYTES + (long) capacity * REFERENCE_BYTES;
        long hashArray = ARRAY_HEADER_BYTES + (long) capacity * Integer.BYTES;
        long stateArray = ARRAY_HEADER_BYTES + capacity;
        return TABLE_OBJECT_BYTES + keyHandleArray + entryHandleArray + hashArray + stateArray;
    }

    @FunctionalInterface
    public interface EntryConsumer {
        void accept(KeyHandle keyHandle, EntryHandle entryHandle);
    }

    @FunctionalInterface
    public interface ScanConsumer {
        boolean accept(KeyHandle keyHandle, EntryHandle entryHandle);
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
            this.directory = directory;
        }

        public KeyHandle keyHandle() {
            ensureActive();
            return KeyHandle.forNative(allocator, keyHandle, hash);
        }

        public long stagedHeapBytes() {
            return directory == null ? 0L : directory.heapBytes;
        }

        private NativeHandle publish() {
            ensureActive();
            NativeHandle published = keyHandle;
            keyHandle = null;
            terminal = true;
            return published;
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
        private final EntryHandle[] handles;
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
            this.handles = new EntryHandle[capacity];
            this.hashes = new int[capacity];
            this.states = new byte[capacity];
            this.heapBytes = heapBytesForCapacity(capacity);
        }
    }
}
