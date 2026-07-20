package yier.bubu.redis.storage.memory.internal.ffm;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.foreign.YierdisFfmAccess;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisFfmRegion;
import yier.bubu.redis.memory.foreign.YierdisFfmSpan;
import yier.bubu.redis.storage.memory.internal.expire.PreparedTtlMutation;
import yier.bubu.redis.storage.memory.internal.expire.YierdisExpireIndex;
import yier.bubu.redis.storage.memory.internal.hash.HashCapacityPolicy;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMetrics;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkBudget;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkResult;
import yier.bubu.redis.storage.memory.internal.hash.SipHash24;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandleAccess;

public final class YierdisFfmExpireIndex implements YierdisExpireIndex, AutoCloseable, HashTableMaintenanceRegistry.Participant {
    @FunctionalInterface
    public interface RegionAllocator {
        YierdisFfmRegion allocateRegion(String owner, int bytes);
    }

    private static final HashTableWorkBudget ACCESS_REHASH_BUDGET = HashTableWorkBudget.of(1L, Long.MAX_VALUE);
    private static final byte STATE_EMPTY = 0;
    private static final byte STATE_FILLED = 1;
    private static final byte STATE_TOMBSTONE = 2;
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long HANDLE_BYTES = Long.BYTES;
    private static final long TABLE_HEAP_BYTES_ESTIMATE = 512L;

    private final NativeAllocator nativeAllocator;
    private final RegionAllocator regionAllocator;
    private final HashSeed hashSeed;
    private final HashTableMaintenanceRegistry maintenanceRegistry;
    private final HashTableMaintenanceRegistry.Registration maintenanceRegistration;

    private Table table0;
    private Table table1;
    private int rehashIndex = -1;
    private long generation;
    private long completedRehashes;
    private int maximumProbeLength;
    private boolean maintenanceDebt;

    public YierdisFfmExpireIndex(YierdisFfmMemoryRuntime memoryRuntime, NativeAllocator nativeAllocator) {
        this(memoryRuntime, nativeAllocator, HashSeed.random(), memoryRuntime::allocateRegion);
    }

    public YierdisFfmExpireIndex(
            YierdisFfmMemoryRuntime memoryRuntime,
            NativeAllocator nativeAllocator,
            HashSeed hashSeed
    ) {
        this(memoryRuntime, nativeAllocator, hashSeed, memoryRuntime::allocateRegion, null);
    }

    public YierdisFfmExpireIndex(
            YierdisFfmMemoryRuntime memoryRuntime,
            NativeAllocator nativeAllocator,
            HashSeed hashSeed,
            HashTableMaintenanceRegistry maintenanceRegistry
    ) {
        this(memoryRuntime, nativeAllocator, hashSeed, memoryRuntime::allocateRegion, maintenanceRegistry);
    }

    public YierdisFfmExpireIndex(
            YierdisFfmMemoryRuntime memoryRuntime,
            NativeAllocator nativeAllocator,
            RegionAllocator regionAllocator
    ) {
        this(memoryRuntime, nativeAllocator, HashSeed.random(), regionAllocator, null);
    }

    public YierdisFfmExpireIndex(
            YierdisFfmMemoryRuntime memoryRuntime,
            NativeAllocator nativeAllocator,
            HashSeed hashSeed,
            RegionAllocator regionAllocator
    ) {
        this(memoryRuntime, nativeAllocator, hashSeed, regionAllocator, null);
    }

    public YierdisFfmExpireIndex(
            YierdisFfmMemoryRuntime memoryRuntime,
            NativeAllocator nativeAllocator,
            HashSeed hashSeed,
            RegionAllocator regionAllocator,
            HashTableMaintenanceRegistry maintenanceRegistry
    ) {
        Objects.requireNonNull(memoryRuntime, "memoryRuntime");
        this.nativeAllocator = Objects.requireNonNull(nativeAllocator, "nativeAllocator");
        this.hashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
        this.regionAllocator = Objects.requireNonNull(regionAllocator, "regionAllocator");
        this.maintenanceRegistry = maintenanceRegistry;
        this.maintenanceRegistration = maintenanceRegistry == null ? null : maintenanceRegistry.registration(this);
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

    public HashTableMetrics metrics() {
        Table active = table1 == null ? table0 : table1;
        int activeSize = active == null ? 0 : active.size;
        int activeFilled = active == null ? 0 : active.used;
        return new HashTableMetrics(
                active == null ? 0 : active.capacity,
                tableSize(table0) + tableSize(table1),
                activeFilled,
                activeFilled - activeSize,
                table1 != null,
                table1 == null || table0 == null ? 0 : table0.capacity,
                table1 == null ? 0 : rehashIndex,
                generation,
                completedRehashes,
                maximumProbeLength
        );
    }

    @Override
    public boolean hasMaintenanceDebt() {
        return maintenanceDebt || table1 != null;
    }

    @Override
    public long estimatedMaintenanceGrowthBytes() {
        if (table0 == null || table1 != null) {
            return 0L;
        }
        HashCapacityPolicy.Decision decision = maintenanceDecision(table0);
        return decision.action() == HashCapacityPolicy.Action.NONE
                ? 0L
                : MemoryUsageSnapshot.addSaturating(
                        tableRegionBytes(decision.targetCapacity()),
                        tableHeapBytes(decision.targetCapacity())
                );
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
                return requirePending().stagedNonNativeGrowthBytes();
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
                    throw new IllegalStateException("staged expiry-index maintenance resize is closed");
                }
                return pending;
            }
        };
    }

    public StagedResize stageMaintenanceResize() {
        Table source = table0;
        if (source == null || table1 != null) {
            return null;
        }
        HashCapacityPolicy.Decision decision = maintenanceDecision(source);
        if (decision.action() == HashCapacityPolicy.Action.NONE) {
            maintenanceDebt = false;
            refreshMaintenanceRegistration();
            return null;
        }
        return new StagedResize(source, new Table(decision.targetCapacity()));
    }

    public void publishStagedResize(StagedResize staged) {
        Objects.requireNonNull(staged, "staged");
        staged.ensureActive();
        if (table1 != null || table0 != staged.source) {
            throw new IllegalStateException("staged expiry-index resize is no longer current");
        }
        publishStagedRehash(staged.publish());
    }

    public long heapEstimatedBytes() {
        return MemoryUsageSnapshot.addSaturating(tableHeapBytes(table0), tableHeapBytes(table1));
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

    public MemoryUsageSnapshot memoryUsage() {
        long heap = MemoryUsageSnapshot.addSaturating(
                tableHeapBytes(table0),
                tableHeapBytes(table1)
        );
        long nativeBytes = nativeBytes();
        return new MemoryUsageSnapshot(heap, 0L, nativeBytes, nativeBytes, 0L);
    }

    public long estimatedInsertFfmRegionGrowthBytes() {
        int capacity = tableCapacityAllocatedByNextInsert();
        return capacity <= 0 ? 0L : tableRegionBytes(capacity);
    }

    public long estimatedInsertHeapGrowthBytes() {
        int capacity = tableCapacityAllocatedByNextInsert();
        return capacity <= 0 ? 0L : tableHeapBytes(capacity);
    }

    public long estimatedSetReplacementNonNativeGrowthBytes(boolean addingNewTtl) {
        int currentSize = tableSize(table0) + tableSize(table1);
        int replacementSize = currentSize + (addingNewTtl ? 1 : 0);
        if (replacementSize <= 0) {
            replacementSize = 1;
        }
        int capacity = tableSizeFor(replacementSize);
        return MemoryUsageSnapshot.addSaturating(tableRegionBytes(capacity), tableHeapBytes(capacity));
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
        return getWithoutRehash(keyHandle);
    }

    @Override
    public Long getForScan(KeyHandle keyHandle) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        return getWithoutRehash(keyHandle);
    }

    private Long getWithoutRehash(KeyHandle keyHandle) {
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
        rehashStep();
        int total = size();
        if (total == 0) {
            return null;
        }
        if (table1 == null) {
            return randomKeyFromTable(table0);
        }
        int r = ThreadLocalRandom.current().nextInt(total);
        byte[] key = r < table0.size ? randomKeyFromTable(table0) : randomKeyFromTable(table1);
        if (key != null) {
            return key;
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

    @Override
    public void clear() {
        Table empty = new Table(HashCapacityPolicy.MIN_CAPACITY);
        clearTable(table0);
        clearTable(table1);
        table0 = empty;
        table1 = null;
        rehashIndex = -1;
        maintenanceDebt = false;
        refreshMaintenanceRegistration();
        generation++;
    }

    @Override
    public void close() {
        clearTable(table0);
        clearTable(table1);
        table0 = null;
        table1 = null;
        rehashIndex = -1;
        maintenanceDebt = false;
        refreshMaintenanceRegistration();
    }

    @Override
    public void setExpireAtMillis(KeyHandle keyHandle, long expireAtMillis) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        long keyRawHandle = keyRawHandle(keyHandle);
        ensureTable0();
        rehashStep();

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

        maybeStartRehashForInsert(keyRawHandle, h);
        if (table1 != null) {
            insertNewIntoTable1(keyRawHandle, h, expireAtMillis);
            return;
        }
        insertNewIntoTable(table0, keyRawHandle, h, expireAtMillis);
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

    @Override
    public PreparedTtlMutation prepareSetExpireAtMillis(KeyHandle keyHandle, long expireAtMillis) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        long targetRawHandle = keyRawHandle(keyHandle);
        int targetHash = hash(keyHandle);
        return prepareReplacement(targetRawHandle, targetHash, expireAtMillis);
    }

    @Override
    public PreparedTtlMutation prepareRemoveExpire(KeyHandle keyHandle) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        int targetHash = hash(keyHandle);
        int index = findIndex(table0, keyHandle, targetHash);
        if (index >= 0) {
            return new PreparedRemove(table0, index);
        }
        index = findIndex(table1, keyHandle, targetHash);
        if (index >= 0) {
            return new PreparedRemove(table1, index);
        }
        return PreparedTtlMutation.NONE;
    }

    private void ensureTable0() {
        if (table0 == null) {
            table0 = new Table(HashCapacityPolicy.MIN_CAPACITY);
        }
    }

    private int tableCapacityAllocatedByNextInsert() {
        Table t0 = table0;
        if (t0 == null) {
            return HashCapacityPolicy.MIN_CAPACITY;
        }
        Table t1 = table1;
        if (t1 != null) {
            return targetCapacityForPotentialInsert(t1);
        }
        return targetCapacityForPotentialInsert(t0);
    }

    private static int targetCapacityForPotentialInsert(Table table) {
        int projectedFilled = Math.min(table.capacity, table.used + 1);
        int projectedSize = table.size + 1;
        int projectedTombstones = projectedFilled - projectedSize;
        if (projectedTombstones < 0) {
            return 0;
        }
        HashCapacityPolicy.Decision decision = HashCapacityPolicy.nextAction(
                table.capacity,
                projectedSize,
                projectedFilled,
                projectedTombstones
        );
        return decision.action() == HashCapacityPolicy.Action.GROW
                        || decision.action() == HashCapacityPolicy.Action.COMPACT
                ? decision.targetCapacity()
                : 0;
    }

    private long tableBytes(Table table) {
        if (table == null) {
            return 0L;
        }
        return tableRegionBytes(table.capacity);
    }

    private long tableHeapBytes(Table table) {
        if (table == null) {
            return 0L;
        }
        return tableHeapBytes(table.capacity);
    }

    private static long tableRegionBytes(int capacity) {
        long statesBytes = capacity;
        long hashesBytes = (long) capacity * Integer.BYTES;
        long expireAtBytes = (long) capacity * Long.BYTES;
        return MemoryUsageSnapshot.addSaturating(
                MemoryUsageSnapshot.addSaturating(statesBytes, hashesBytes),
                expireAtBytes
        );
    }

    private static long tableHeapBytes(int capacity) {
        return MemoryUsageSnapshot.addSaturating(
                TABLE_HEAP_BYTES_ESTIMATE,
                ARRAY_HEADER_BYTES + (long) capacity * HANDLE_BYTES
        );
    }

    private void clearTable(Table table) {
        if (table == null) {
            return;
        }
        for (int i = 0; i < table.capacity; i++) {
            if (table.stateAt(i) != STATE_FILLED) {
                continue;
            }
            releaseKeyFromIndex(table.keyRawHandles[i]);
            table.keyRawHandles[i] = 0L;
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
                return keyHandle(table.keyRawHandles[idx], table.hashAt(idx));
            }
        }
        for (int i = 0; i < table.capacity; i++) {
            int idx = (start + i) & mask;
            if (table.stateAt(idx) == STATE_FILLED) {
                return keyHandle(table.keyRawHandles[idx], table.hashAt(idx));
            }
        }
        return null;
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
                return copyKey(table.keyRawHandles[idx]);
            }
        }
        for (int i = 0; i < table.capacity; i++) {
            int idx = (start + i) & mask;
            if (table.stateAt(idx) == STATE_FILLED) {
                return copyKey(table.keyRawHandles[idx]);
            }
        }
        return null;
    }

    private void maybeStartRehashForInsert(long keyRawHandle, int hash) {
        if (table1 != null) {
            return;
        }
        Table t0 = table0;
        int location = findOrInsertLocation(t0, keyRawHandle, hash);
        if (location >= 0) {
            throw new IllegalStateException("expire index key already exists during resize preparation");
        }
        int insertionIndex = -location - 1;
        int projectedSize = t0.size + 1;
        int projectedFilled = t0.used + (t0.stateAt(insertionIndex) == STATE_EMPTY ? 1 : 0);
        int projectedTombstones = projectedFilled - projectedSize;
        HashCapacityPolicy.Decision decision = HashCapacityPolicy.nextAction(
                t0.capacity,
                projectedSize,
                projectedFilled,
                projectedTombstones
        );
        if (decision.action() == HashCapacityPolicy.Action.NONE) {
            return;
        }
        startRehash(decision.targetCapacity());
    }

    private HashCapacityPolicy.Decision maintenanceDecision(Table table) {
        return HashCapacityPolicy.nextAction(
                table.capacity,
                table.size,
                table.used,
                table.used - table.size
        );
    }

    private void recordMaintenanceDebt() {
        maintenanceDebt = table1 != null
                || (table0 != null && maintenanceDecision(table0).action() != HashCapacityPolicy.Action.NONE);
        refreshMaintenanceRegistration();
    }

    private void startRehash(int capacity) {
        if (table1 != null) {
            throw new IllegalStateException("expire index is already rehashing");
        }
        publishStagedRehash(new Table(capacity));
    }

    private void publishStagedRehash(Table staged) {
        if (table1 != null) {
            throw new IllegalStateException("expire index is already rehashing");
        }
        table1 = Objects.requireNonNull(staged, "staged");
        rehashIndex = 0;
        maintenanceDebt = true;
        refreshMaintenanceRegistration();
        generation++;
    }

    private void refreshMaintenanceRegistration() {
        if (maintenanceRegistration == null) {
            return;
        }
        if (maintenanceDebt || table1 != null) {
            if (!maintenanceRegistration.registered()) {
                maintenanceRegistry.register(maintenanceRegistration);
            }
        } else {
            maintenanceRegistry.unregister(maintenanceRegistration);
        }
    }

    private PreparedTtlMutation prepareReplacement(long targetRawHandle, int targetHash, Long expireAtMillis) {
        boolean targetExists = containsRawHandle(table0, targetRawHandle, targetHash)
                || containsRawHandle(table1, targetRawHandle, targetHash);
        int currentSize = tableSize(table0) + tableSize(table1);
        int replacementSize = currentSize - (targetExists ? 1 : 0) + (expireAtMillis == null ? 0 : 1);
        Table replacement = null;
        if (replacementSize > 0) {
            int capacity = tableSizeFor(replacementSize);
            replacement = new Table(capacity);
            copyTableExcluding(table0, replacement, targetRawHandle, targetHash);
            copyTableExcluding(table1, replacement, targetRawHandle, targetHash);
            if (expireAtMillis != null) {
                insertIntoTable(replacement, targetRawHandle, targetHash, expireAtMillis);
            }
            if (replacement.size != replacementSize) {
                replacement.close();
                throw new IllegalStateException("prepared expire replacement size mismatch");
            }
        }
        long stagedBytes = replacement == null
                ? 0L
                : MemoryUsageSnapshot.addSaturating(
                        tableRegionBytes(replacement.capacity),
                        tableHeapBytes(replacement.capacity)
                );
        return new PreparedReplacement(replacement, stagedBytes);
    }

    private void copyTableExcluding(Table source, Table target, long excludedRawHandle, int excludedHash) {
        if (source == null || target == null) {
            return;
        }
        for (int i = 0; i < source.capacity; i++) {
            if (source.stateAt(i) != STATE_FILLED) {
                continue;
            }
            long rawHandle = source.keyRawHandles[i];
            int hash = source.hashAt(i);
            if (hash == excludedHash && rawHandle == excludedRawHandle) {
                continue;
            }
            insertIntoTable(target, rawHandle, hash, source.expireAt(i));
        }
    }

    private boolean containsRawHandle(Table table, long rawHandle, int hash) {
        if (table == null) {
            return false;
        }
        int index = findIndex(table, rawHandle, hash);
        return index >= 0;
    }

    private int findIndex(Table table, long rawHandle, int hash) {
        if (table == null) {
            return -1;
        }
        int mask = table.capacity - 1;
        int idx = hash & mask;
        for (int probes = 1; probes <= table.capacity; probes++) {
            recordProbe(probes);
            byte state = table.stateAt(idx);
            if (state == STATE_EMPTY) {
                return -1;
            }
            if (state == STATE_FILLED && table.hashAt(idx) == hash && table.keyRawHandles[idx] == rawHandle) {
                return idx;
            }
            idx = (idx + 1) & mask;
        }
        return -1;
    }

    private static int tableSize(Table table) {
        return table == null ? 0 : table.size;
    }

    private void rehashStep() {
        advanceRehash(ACCESS_REHASH_BUDGET);
    }

    @Override
    public HashTableWorkResult advanceRehash(HashTableWorkBudget budget) {
        Objects.requireNonNull(budget, "budget");
        if (table1 == null) {
            return new HashTableWorkResult(0L, 0L, true, HashTableWorkResult.StopReason.NOT_REHASHING);
        }

        long inspected = 0L;
        long migrated = 0L;
        long startedAt = System.nanoTime();
        Table source = table0;
        while (rehashIndex < source.capacity) {
            if (inspected >= budget.maxInspectedSlots()) {
                return new HashTableWorkResult(inspected, migrated, false, HashTableWorkResult.StopReason.SLOT_LIMIT);
            }
            if (timeLimitReached(startedAt, budget.timeLimitNanos())) {
                return new HashTableWorkResult(inspected, migrated, false, HashTableWorkResult.StopReason.TIME_LIMIT);
            }

            int index = rehashIndex++;
            inspected++;
            if (source.stateAt(index) == STATE_FILLED) {
                moveOldSlotToTable1(source, index);
                migrated++;
            }
        }

        finishRehash();
        return new HashTableWorkResult(inspected, migrated, true, HashTableWorkResult.StopReason.COMPLETE);
    }

    private void finishRehash() {
        Table old0 = table0;
        Table new0 = table1;
        table0 = new0;
        table1 = null;
        rehashIndex = -1;
        completedRehashes++;
        generation++;
        recordMaintenanceDebt();
        Throwable failure = closeTable(old0, null);
        if (failure != null) {
            rethrow(failure);
        }
    }

    private void moveOldSlotToTable1(Table source, int sourceIndex) {
        long rawHandle = source.keyRawHandles[sourceIndex];
        int hash = source.hashAt(sourceIndex);
        long expireAtMillis = source.expireAt(sourceIndex);
        ensureTable1CapacityForInsert(rawHandle, hash);

        source.setState(sourceIndex, STATE_TOMBSTONE);
        source.setHash(sourceIndex, 0);
        source.setExpireAt(sourceIndex, 0L);
        source.keyRawHandles[sourceIndex] = 0L;
        source.size--;
        insertIntoTable(table1, rawHandle, hash, expireAtMillis);
    }

    private void insertNewIntoTable1(long rawHandle, int hash, long expireAtMillis) {
        ensureTable1CapacityForInsert(rawHandle, hash);
        insertNewIntoTable(table1, rawHandle, hash, expireAtMillis);
    }

    private void insertNewIntoTable(Table table, long rawHandle, int hash, long expireAtMillis) {
        retainKeyForIndex(rawHandle);
        insertIntoTable(table, rawHandle, hash, expireAtMillis);
    }

    private void insertIntoTable(Table table, long rawHandle, int hash, long expireAtMillis) {
        int loc = findOrInsertLocation(table, rawHandle, hash);
        int insertAt = -loc - 1;
        if (table.stateAt(insertAt) == STATE_EMPTY) {
            table.used++;
        }
        table.setState(insertAt, STATE_FILLED);
        table.setHash(insertAt, hash);
        table.keyRawHandles[insertAt] = rawHandle;
        table.setExpireAt(insertAt, expireAtMillis);
        table.size++;
    }

    private void ensureTable1CapacityForInsert(long rawHandle, int hash) {
        Table target = table1;
        int location = findOrInsertLocation(target, rawHandle, hash);
        if (location >= 0) {
            return;
        }
        int insertionIndex = -location - 1;
        int projectedSize = target.size + 1;
        int projectedFilled = target.used + (target.stateAt(insertionIndex) == STATE_EMPTY ? 1 : 0);
        int projectedTombstones = projectedFilled - projectedSize;
        HashCapacityPolicy.Decision decision = HashCapacityPolicy.nextAction(
                target.capacity,
                projectedSize,
                projectedFilled,
                projectedTombstones
        );
        if (decision.action() == HashCapacityPolicy.Action.GROW
                || decision.action() == HashCapacityPolicy.Action.COMPACT) {
            resizeTable1(decision.targetCapacity());
        }
    }

    private void resizeTable1(int capacity) {
        Table old = table1;
        int oldSize = old.size;
        Table next = new Table(capacity);
        for (int i = 0; i < old.capacity; i++) {
            if (old.stateAt(i) != STATE_FILLED) {
                continue;
            }
            long rawHandle = old.keyRawHandles[i];
            int hash = old.hashAt(i);
            long expireAt = old.expireAt(i);
            int loc = -findOrInsertLocation(next, rawHandle, hash) - 1;
            next.setState(loc, STATE_FILLED);
            next.setHash(loc, hash);
            next.keyRawHandles[loc] = rawHandle;
            next.setExpireAt(loc, expireAt);
            next.size++;
            next.used++;
        }
        if (next.size != oldSize) {
            old.close();
            next.close();
            throw new IllegalStateException("expire-index destination resize size mismatch");
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
        return SipHash24.foldToInt(SipHash24.hash(hashSeed, key));
    }

    private int hash(BytesView key) {
        return SipHash24.foldToInt(SipHash24.hash(hashSeed, key));
    }

    private int hash(KeyHandle keyHandle) {
        return SipHash24.foldToInt(SipHash24.hash(hashSeed, keyHandle));
    }

    private int findIndex(Table table, byte[] key, int hash) {
        if (table == null) {
            return -1;
        }
        int mask = table.capacity - 1;
        int idx = hash & mask;
        for (int probes = 1; probes <= table.capacity; probes++) {
            recordProbe(probes);
            byte state = table.stateAt(idx);
            if (state == STATE_EMPTY) {
                return -1;
            }
            if (state == STATE_FILLED && table.hashAt(idx) == hash && equalsBytes(table.keyRawHandles[idx], key)) {
                return idx;
            }
            idx = (idx + 1) & mask;
        }
        return -1;
    }

    private int findIndex(Table table, BytesView key, int hash) {
        if (table == null) {
            return -1;
        }
        int mask = table.capacity - 1;
        int idx = hash & mask;
        for (int probes = 1; probes <= table.capacity; probes++) {
            recordProbe(probes);
            byte state = table.stateAt(idx);
            if (state == STATE_EMPTY) {
                return -1;
            }
            if (state == STATE_FILLED && table.hashAt(idx) == hash && equalsBytes(table.keyRawHandles[idx], key)) {
                return idx;
            }
            idx = (idx + 1) & mask;
        }
        return -1;
    }

    private int findIndex(Table table, KeyHandle keyHandle, int hash) {
        if (table == null) {
            return -1;
        }
        long handleRaw = keyRawHandleOrZero(keyHandle);
        int mask = table.capacity - 1;
        int idx = hash & mask;
        for (int probes = 1; probes <= table.capacity; probes++) {
            recordProbe(probes);
            byte state = table.stateAt(idx);
            if (state == STATE_EMPTY) {
                return -1;
            }
            if (state == STATE_FILLED && table.hashAt(idx) == hash) {
                long storedRaw = table.keyRawHandles[idx];
                if (handleRaw != 0L && storedRaw == handleRaw) {
                    return idx;
                }
                if (equalsBytes(storedRaw, keyHandle)) {
                    return idx;
                }
            }
            idx = (idx + 1) & mask;
        }
        return -1;
    }

    private int findOrInsertLocation(Table table, long rawHandle, int hash) {
        int mask = table.capacity - 1;
        int idx = hash & mask;
        int firstTombstone = -1;
        for (int probes = 1; probes <= table.capacity; probes++) {
            recordProbe(probes);
            byte state = table.stateAt(idx);
            if (state == STATE_EMPTY) {
                int target = firstTombstone >= 0 ? firstTombstone : idx;
                return -(target + 1);
            }
            if (state == STATE_TOMBSTONE) {
                if (firstTombstone < 0) {
                    firstTombstone = idx;
                }
            } else if (table.hashAt(idx) == hash && table.keyRawHandles[idx] == rawHandle) {
                return idx;
            }
            idx = (idx + 1) & mask;
        }
        if (firstTombstone >= 0) {
            return -(firstTombstone + 1);
        }
        throw new IllegalStateException("expire index has no insertion slot");
    }

    private long keyRawHandle(KeyHandle handle) {
        long rawHandle = keyRawHandleOrZero(handle);
        if (rawHandle == 0L) {
            throw new IllegalArgumentException("unsupported KeyHandle: " + handle.getClass().getName());
        }
        return rawHandle;
    }

    private static long keyRawHandleOrZero(KeyHandle handle) {
        NativeHandle nativeHandle = KeyHandleAccess.allocatorNativeHandleOrNull(handle);
        return nativeHandle == null ? 0L : nativeHandle.raw();
    }

    private static int tableSizeFor(int expectedSize) {
        int capacity = HashCapacityPolicy.MIN_CAPACITY;
        while (expectedSize > capacity - capacity / 4) {
            if (capacity == HashCapacityPolicy.MAX_CAPACITY) {
                HashCapacityPolicy.nextAction(capacity, capacity, capacity, 0);
            }
            capacity <<= 1;
        }
        return capacity;
    }

    private void recordProbe(int probes) {
        if (probes > maximumProbeLength) {
            maximumProbeLength = probes;
        }
    }

    private static boolean timeLimitReached(long startedAt, long timeLimitNanos) {
        return timeLimitNanos != Long.MAX_VALUE && System.nanoTime() - startedAt >= timeLimitNanos;
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
            long rawHandle = current.keyRawHandles[index];
            current.setState(index, STATE_TOMBSTONE);
            current.setHash(index, 0);
            current.setExpireAt(index, 0L);
            current.keyRawHandles[index] = 0L;
            current.size--;
            releaseKeyFromIndex(rawHandle);
            recordMaintenanceDebt();
        }
    }

    private final class PreparedReplacement implements PreparedTtlMutation {
        private Table replacement;
        private Table oldTable0;
        private Table oldTable1;
        private final long stagedBytes;
        private boolean committed;
        private boolean released;
        private boolean aborted;

        private PreparedReplacement(Table replacement, long stagedBytes) {
            this.replacement = replacement;
            this.stagedBytes = stagedBytes;
        }

        @Override
        public long stagedNonNativeGrowthBytes() {
            return stagedBytes;
        }

        @Override
        public void commit() {
            if (aborted) {
                throw new IllegalStateException("prepared ttl mutation is aborted");
            }
            if (committed) {
                throw new IllegalStateException("prepared ttl mutation is already committed");
            }
            oldTable0 = table0;
            oldTable1 = table1;
            table0 = replacement;
            table1 = null;
            rehashIndex = -1;
            replacement = null;
            committed = true;
            maintenanceDebt = false;
            refreshMaintenanceRegistration();
            generation++;
        }

        @Override
        public void releaseSuperseded() {
            if (!committed || released) {
                return;
            }
            Throwable failure = null;
            failure = closeTable(oldTable0, failure);
            oldTable0 = null;
            failure = closeTable(oldTable1, failure);
            oldTable1 = null;
            if (failure != null) {
                rethrow(failure);
            }
            released = true;
        }

        @Override
        public void abort() {
            if (committed || aborted) {
                return;
            }
            aborted = true;
            Throwable failure = closeTable(replacement, null);
            replacement = null;
            if (failure != null) {
                rethrow(failure);
            }
        }
    }

    private final class PreparedRemove implements PreparedTtlMutation {
        private final Table table;
        private final int index;
        private long removedRawHandle;
        private boolean committed;
        private boolean aborted;

        private PreparedRemove(Table table, int index) {
            this.table = Objects.requireNonNull(table, "table");
            this.index = index;
        }

        @Override
        public long stagedNonNativeGrowthBytes() {
            return 0L;
        }

        @Override
        public void commit() {
            if (aborted) {
                throw new IllegalStateException("prepared ttl remove is aborted");
            }
            if (committed) {
                throw new IllegalStateException("prepared ttl remove is already committed");
            }
            if (table.stateAt(index) != STATE_FILLED) {
                throw new IllegalStateException("prepared ttl remove target is no longer filled");
            }
            removedRawHandle = table.keyRawHandles[index];
            table.setState(index, STATE_TOMBSTONE);
            table.setHash(index, 0);
            table.setExpireAt(index, 0L);
            table.keyRawHandles[index] = 0L;
            table.size--;
            recordMaintenanceDebt();
            committed = true;
        }

        @Override
        public void releaseSuperseded() {
            long rawHandle = removedRawHandle;
            removedRawHandle = 0L;
            releaseKeyFromIndex(rawHandle);
        }

        @Override
        public void abort() {
            if (committed || aborted) {
                return;
            }
            aborted = true;
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

        public long stagedNonNativeGrowthBytes() {
            ensureActive();
            return MemoryUsageSnapshot.addSaturating(
                    tableRegionBytes(replacement.capacity),
                    tableHeapBytes(replacement.capacity)
            );
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
                throw new IllegalStateException("staged expiry-index resize is closed");
            }
        }

        @Override
        public void close() {
            if (terminal) {
                return;
            }
            terminal = true;
            Table current = replacement;
            replacement = null;
            Throwable failure = closeTable(current, null);
            if (failure != null) {
                rethrow(failure);
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
        private final long[] keyRawHandles;

        private int size;
        private int used;

        private Table(int capacity) {
            this.capacity = capacity;
            YierdisFfmRegion stagedStatesRegion = null;
            YierdisFfmRegion stagedHashesRegion = null;
            YierdisFfmRegion stagedExpireAtRegion = null;
            YierdisFfmSpan stagedStates = null;
            YierdisFfmSpan stagedHashes = null;
            YierdisFfmSpan stagedExpireAt = null;
            long[] stagedKeyRawHandles = null;
            try {
                stagedStatesRegion = regionAllocator.allocateRegion("ffm-expire-states", capacity);
                stagedStates = stagedStatesRegion.span(0, capacity);
                stagedHashesRegion = regionAllocator.allocateRegion("ffm-expire-hashes", capacity * Integer.BYTES);
                stagedHashes = stagedHashesRegion.span(0, capacity * Integer.BYTES);
                stagedExpireAtRegion = regionAllocator.allocateRegion("ffm-expire-values", capacity * Long.BYTES);
                stagedExpireAt = stagedExpireAtRegion.span(0, capacity * Long.BYTES);
                stagedKeyRawHandles = new long[capacity];
            } catch (RuntimeException | Error failure) {
                closeRegion(stagedExpireAtRegion, failure);
                closeRegion(stagedHashesRegion, failure);
                closeRegion(stagedStatesRegion, failure);
                throw failure;
            }
            this.statesRegion = stagedStatesRegion;
            this.states = stagedStates;
            this.hashesRegion = stagedHashesRegion;
            this.hashes = stagedHashes;
            this.expireAtRegion = stagedExpireAtRegion;
            this.expireAt = stagedExpireAt;
            this.keyRawHandles = stagedKeyRawHandles;
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
            Throwable failure = null;
            failure = closeRegion(statesRegion, failure);
            failure = closeRegion(hashesRegion, failure);
            failure = closeRegion(expireAtRegion, failure);
            if (failure != null) {
                rethrow(failure);
            }
        }
    }

    private Throwable closeTable(Table table, Throwable failure) {
        if (table == null) {
            return failure;
        }
        try {
            table.close();
            return failure;
        } catch (RuntimeException | Error closeFailure) {
            return addFailure(failure, closeFailure);
        }
    }

    private static Throwable closeRegion(YierdisFfmRegion region, Throwable failure) {
        if (region == null) {
            return failure;
        }
        try {
            region.close();
        } catch (RuntimeException | Error closeFailure) {
            failure = addFailure(failure, closeFailure);
        }
        return failure;
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

    private boolean equalsBytes(long rawHandle, byte[] key) {
        try (NativeObjectView stored = nativeAllocator.resolveRaw(rawHandle, NativeAccessMode.READ_ONLY)) {
            return stored.size() == key.length
                    && stored.contentEquals(0, key, 0, key.length);
        }
    }

    private boolean equalsBytes(long rawHandle, BytesView key) {
        try (NativeObjectView stored = nativeAllocator.resolveRaw(rawHandle, NativeAccessMode.READ_ONLY)) {
            int length = key.length();
            if (stored.size() != length) {
                return false;
            }
            for (int i = 0; i < length; i++) {
                if (stored.getByte(i) != key.getByte(i)) {
                    return false;
                }
            }
            return true;
        }
    }

    private boolean equalsBytes(long rawHandle, KeyHandle key) {
        try (NativeObjectView stored = nativeAllocator.resolveRaw(rawHandle, NativeAccessMode.READ_ONLY)) {
            int length = key.length();
            if (stored.size() != length) {
                return false;
            }
            for (int i = 0; i < length; i++) {
                if (stored.getByte(i) != key.getByte(i)) {
                    return false;
                }
            }
            return true;
        }
    }

    private KeyHandle keyHandle(long rawHandle, int hash) {
        return KeyHandle.forNative(nativeAllocator, NativeHandle.fromRaw(rawHandle), hash);
    }

    private byte[] copyKey(long rawHandle) {
        try (NativeObjectView stored = nativeAllocator.resolveRaw(rawHandle, NativeAccessMode.READ_ONLY)) {
            byte[] copy = new byte[stored.size()];
            stored.getBytes(0, copy, 0, copy.length);
            return copy;
        }
    }

    private static void retainKeyForIndex(long rawHandle) {
        NativeHandle.requireValidRaw(rawHandle);
        if (rawHandle == 0L) {
            throw new IllegalArgumentException("expire index key handle must not be null");
        }
        // KEY_BYTES 由 NativeKeyDirectory 唯一拥有；expire index 只借用 stable handle，不能用长期 pin 代替所有权协调。
    }

    private static void releaseKeyFromIndex(long rawHandle) {
        if (rawHandle != 0L) {
            NativeHandle.requireValidRaw(rawHandle);
        }
    }
}
