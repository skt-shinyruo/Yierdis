package yier.bubu.redis.storage.memory.internal.entry;

import static yier.bubu.redis.common.memory.MemoryUsageSnapshot.addSaturating;

import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.ByteValue;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.hash.SipHash24;
import yier.bubu.redis.storage.memory.internal.value.HashValue;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public final class HashRoot implements AutoCloseable {
    private final NativeCollectionRootTable<HashValue> hashes;
    private final HashSeed hashSeed;
    private final HashTableMaintenanceRegistry maintenanceRegistry;
    private boolean closed;

    public HashRoot(
            StableMemoryBackend allocator,
            HashSeed hashSeed,
            HashTableMaintenanceRegistry maintenanceRegistry
    ) {
        this.hashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
        this.maintenanceRegistry = maintenanceRegistry;
        this.hashes = new NativeCollectionRootTable<>(
                Objects.requireNonNull(allocator, "allocator"),
                NativeObjectKind.HASH_ROOT,
                "hash"
        );
    }

    StableMemoryBackend allocator() {
        return hashes.allocator();
    }

    public synchronized ValueEncoding encoding(ValueHandle handle) {
        return requireHash(handle).encoding();
    }

    public synchronized boolean contains(ValueHandle handle) {
        ensureOpen();
        return hashes.contains(handle);
    }

    public synchronized ValueHandle create() {
        ensureOpen();
        return hashes.create(ignored -> newHashValue());
    }

    public synchronized ValueHandle build(List<byte[]> finalFieldValuePairs) {
        ensureOpen();
        Objects.requireNonNull(finalFieldValuePairs, "finalFieldValuePairs");
        ValueHandle handle = create();
        boolean built = false;
        try {
            HashValue value = requireHash(handle);
            try {
                value.loadForBuild(finalFieldValuePairs);
            } finally {
                hashes.refreshAdapter(handle);
            }
            built = true;
            return handle;
        } finally {
            if (!built) {
                release(handle);
            }
        }
    }

    public synchronized PreparedSetResult prepareSet(ValueHandle source, List<byte[]> fieldValuePairs) {
        return prepareSet(planSet(source, fieldValuePairs));
    }

    public synchronized SetPlan planSet(ValueHandle source, List<byte[]> fieldValuePairs) {
        ensureOpen();
        Objects.requireNonNull(fieldValuePairs, "fieldValuePairs");
        if (source != null) {
            HashValue sourceValue = requireHash(source);
            if (sourceValue.encoding() == ValueEncoding.HASH_HT) {
                HashValue.HashTableSetPlan hashTablePlan = sourceValue.planHashTableSet(fieldValuePairs);
                return new SetPlan(
                        source,
                        hashTablePlan,
                        null,
                        hashTablePlan.added(),
                        hashTablePlan.nativeAllocationSizes()
                );
            }
        }
        MergedFieldValuePairs merged = mergeFieldValuePairs(source, fieldValuePairs);
        return new SetPlan(
                source,
                null,
                merged.values(),
                merged.added(),
                HashValue.preparedBuildNativeAllocationSizes(merged.values())
        );
    }

    public synchronized PreparedSetResult prepareSet(SetPlan plan) {
        ensureOpen();
        Objects.requireNonNull(plan, "plan");
        if (plan.hashTablePlan() != null) {
            HashValue sourceValue = requireHash(plan.source());
            HashValue.PreparedHashTableSet delta = sourceValue.prepareHashTableSet(plan.hashTablePlan());
            return new PreparedSetResult(plan.source(), delta.added(), delta.changedAny(), delta);
        }
        return new PreparedSetResult(build(plan.replacementPairs()), plan.added(), true, null);
    }

    public synchronized int[] preparedSetNativeAllocationSizes(
            ValueHandle source,
            List<byte[]> fieldValuePairs
    ) {
        return planSet(source, fieldValuePairs).nativeAllocationSizes();
    }

    public synchronized int[] preparedSetNativeAllocationSizes(SetPlan plan) {
        ensureOpen();
        Objects.requireNonNull(plan, "plan");
        return plan.nativeAllocationSizes();
    }

    public synchronized ValueHandle store(HashValue value) {
        ensureOpen();
        Objects.requireNonNull(value, "value");
        ValueHandle handle = create();
        boolean ok = false;
        try {
            hsetMany(handle, value.hgetallPairs());
            ok = true;
            return handle;
        } finally {
            if (!ok) {
                release(handle);
            }
        }
    }

    public synchronized int hsetMany(ValueHandle handle, List<byte[]> fieldValuePairs) {
        ensureOpen();
        HashValue value = requireHash(handle);
        try {
            return value.hsetMany(fieldValuePairs);
        } finally {
            hashes.refreshAdapter(handle);
        }
    }

    public synchronized int hset(ValueHandle handle, byte[] field, byte[] value) {
        ensureOpen();
        HashValue hash = requireHash(handle);
        try {
            return hash.hset(field, value);
        } finally {
            hashes.refreshAdapter(handle);
        }
    }

    public synchronized PreparedPackedHset preparePackedHset(ValueHandle source, byte[] field, byte[] value) {
        ensureOpen();
        HashValue sourceValue = requireHash(source);
        HashValue.PreparedPackedHset staged = sourceValue.preparePackedHset(field, value);
        if (staged == null) {
            return null;
        }
        try {
            ValueHandle replacement = hashes.create(ignored -> staged.replacement());
            return new PreparedPackedHset(replacement, staged.added());
        } catch (RuntimeException | Error failure) {
            try {
                staged.replacement().close();
            } catch (RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    public synchronized byte[] hget(ValueHandle handle, byte[] field) {
        ensureOpen();
        return requireHash(handle).hget(field);
    }

    public synchronized ByteValue hgetValue(ValueHandle handle, byte[] field) {
        ensureOpen();
        return requireHash(handle).hgetValue(field);
    }

    public synchronized int hdel(ValueHandle handle, List<byte[]> fields) {
        ensureOpen();
        HashValue hash = requireHash(handle);
        try {
            return hash.hdel(fields);
        } finally {
            hashes.refreshAdapter(handle);
        }
    }

    public synchronized int countExistingFields(ValueHandle handle, List<byte[]> fields) {
        ensureOpen();
        return requireHash(handle).countExistingFields(fields);
    }

    public synchronized int hgetallCount(ValueHandle handle) {
        ensureOpen();
        return requireHash(handle).hgetallCount();
    }

    public synchronized List<byte[]> hgetallPairs(ValueHandle handle) {
        ensureOpen();
        return requireHash(handle).hgetallPairs();
    }

    public synchronized void hgetallPairsInto(ValueHandle handle, ByteValueSink out) {
        ensureOpen();
        requireHash(handle).hgetallPairsInto(out);
    }

    public synchronized CollectionScanWindow hscan(
            ValueHandle handle,
            ScanCursorV2 cursor,
            byte[] globPattern,
            int count,
            boolean noValues
    ) {
        ensureOpen();
        return requireHash(handle).hscan(cursor, globPattern, count, noValues);
    }

    public synchronized int[] nativePayloadSizes(ValueHandle handle) {
        ensureOpen();
        return requireHash(handle).nativePayloadSizes();
    }

    public synchronized int size(ValueHandle handle) {
        ensureOpen();
        return requireHash(handle).size();
    }

    public synchronized long estimatedPreparedSetHeapGrowthBytes(
            ValueHandle source,
            List<byte[]> fieldValuePairs
    ) {
        return estimatedPreparedSetHeapGrowthBytes(
                planSet(source, fieldValuePairs)
        );
    }

    public synchronized long estimatedPreparedSetHeapGrowthBytes(
            SetPlan plan
    ) {
        ensureOpen();
        Objects.requireNonNull(plan, "plan");
        if (plan.hashTablePlan() != null) {
            return plan.hashTablePlan().stagedHeapBytes();
        }
        long replacementHeapBytes = HashValue.preparedNewHeapUpperBound(plan.replacementPairs());
        return hashes.estimatedNewAdapterHeapGrowthBytes(replacementHeapBytes);
    }

    public synchronized long retainedHeapBytes() {
        ensureOpen();
        long registryHeapBytes = maintenanceRegistry == null ? 0L : maintenanceRegistry.heapEstimatedBytes();
        return addSaturating(hashes.heapBytes(), registryHeapBytes);
    }

    public synchronized long positiveRetainedHeapGrowthBytes(long before) {
        return positiveDelta(retainedHeapBytes(), before);
    }

    public synchronized long estimatedBytes(ValueHandle handle) {
        ensureOpen();
        return requireHash(handle).estimatedBytes();
    }

    public synchronized long nativeBytes() {
        return hashes.adapterBytes(HashValue::estimatedBytes);
    }

    public synchronized long heapBytes() {
        ensureOpen();
        return hashes.heapBytes();
    }

    public synchronized void forEachNativeHandle(ValueHandle handle, Consumer<NativeHandle> consumer) {
        ensureOpen();
        requireHash(handle).forEachNativeHandle(consumer);
    }

    public synchronized void release(ValueHandle handle) {
        hashes.release(handle);
    }

    public synchronized void releaseExcept(ValueHandle source, ValueHandle retained) {
        ensureOpen();
        HashValue sourceValue = requireHash(source);
        HashValue retainedValue = requireHash(retained);
        sourceValue.releaseExcept(retainedValue);
        retainedValue.activateBorrowedPackedOwnership(sourceValue);
        hashes.release(source);
    }

    public synchronized void discardPackedHset(ValueHandle replacement, ValueHandle source) {
        ensureOpen();
        HashValue replacementValue = requireHash(replacement);
        HashValue sourceValue = requireHash(source);
        replacementValue.releaseExcept(sourceValue);
        hashes.release(replacement);
    }

    public synchronized void clear() {
        ensureOpen();
        hashes.clear();
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        hashes.close();
        closed = true;
    }

    private HashValue requireHash(ValueHandle handle) {
        return hashes.require(handle);
    }

    private HashValue newHashValue() {
        return new HashValue(hashes.allocator(), hashSeed, maintenanceRegistry);
    }

    private MergedFieldValuePairs mergeFieldValuePairs(ValueHandle source, List<byte[]> fieldValuePairs) {
        Objects.requireNonNull(fieldValuePairs, "fieldValuePairs");
        if ((fieldValuePairs.size() & 1) != 0) {
            throw new IllegalArgumentException("fieldValuePairs must contain field/value pairs");
        }
        ArrayList<byte[]> merged = source == null
                ? new ArrayList<>(fieldValuePairs.size())
                : new ArrayList<>(requireHash(source).hgetallPairs());
        FieldPairIndex pairIndex = new FieldPairIndex(
                merged,
                fieldValuePairs.size() / 2,
                hashSeed
        );
        int added = 0;
        for (int index = 0; index < fieldValuePairs.size(); index += 2) {
            byte[] field = Objects.requireNonNull(fieldValuePairs.get(index), "hash field");
            byte[] value = fieldValuePairs.get(index + 1);
            int existingPairIndex = pairIndex.find(field);
            if (existingPairIndex >= 0) {
                merged.set(existingPairIndex + 1, value);
                continue;
            }
            int appendedPairIndex = merged.size();
            merged.add(field);
            merged.add(value);
            pairIndex.add(appendedPairIndex);
            added++;
        }
        return new MergedFieldValuePairs(merged, added);
    }

    private static long positiveDelta(long after, long before) {
        return after > before ? after - before : 0L;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("hash root is closed");
        }
    }

    public record PreparedPackedHset(ValueHandle replacementHandle, int added) {
        public PreparedPackedHset {
            Objects.requireNonNull(replacementHandle, "replacementHandle");
            if (added < 0 || added > 1) {
                throw new IllegalArgumentException("added must be 0 or 1");
            }
        }
    }

    public record SetPlan(
            ValueHandle source,
            HashValue.HashTableSetPlan hashTablePlan,
            List<byte[]> replacementPairs,
            int added,
            int[] allocationSizes
    ) {
        public SetPlan {
            if ((hashTablePlan == null) == (replacementPairs == null)) {
                throw new IllegalArgumentException("set plan must select exactly one representation path");
            }
            if (hashTablePlan != null && source == null) {
                throw new IllegalArgumentException("hash-table delta requires a source handle");
            }
            if (added < 0) {
                throw new IllegalArgumentException("added must be >= 0");
            }
            allocationSizes = Objects.requireNonNull(allocationSizes, "allocationSizes").clone();
        }

        @Override
        public int[] allocationSizes() {
            return allocationSizes.clone();
        }

        public int[] nativeAllocationSizes() {
            return allocationSizes();
        }

        public boolean stableHandle() {
            return hashTablePlan != null;
        }
    }

    public static final class PreparedSetResult implements AutoCloseable {
        private final ValueHandle resultHandle;
        private final int added;
        private final boolean changedAny;
        private final HashValue.PreparedHashTableSet delta;

        private PreparedSetResult(
                ValueHandle resultHandle,
                int added,
                boolean changedAny,
                HashValue.PreparedHashTableSet delta
        ) {
            this.resultHandle = Objects.requireNonNull(resultHandle, "resultHandle");
            if (added < 0) {
                throw new IllegalArgumentException("added must be >= 0");
            }
            this.added = added;
            this.changedAny = changedAny;
            this.delta = delta;
        }

        public ValueHandle replacementHandle() {
            return resultHandle;
        }

        public int added() {
            return added;
        }

        public boolean changedAny() {
            return changedAny;
        }

        public boolean stableHandle() {
            return delta != null;
        }

        public long stagedHeapBytes() {
            return delta == null ? 0L : delta.stagedHeapBytes();
        }

        public void commit() {
            if (delta != null) {
                delta.commit();
            }
        }

        public void releaseSuperseded() {
            if (delta != null) {
                delta.releaseSuperseded();
            }
        }

        @Override
        public void close() {
            if (delta != null) {
                delta.close();
            }
        }
    }

    private record MergedFieldValuePairs(List<byte[]> values, int added) {
    }

    private static final class FieldPairIndex {
        private static final int MAX_CAPACITY = 1 << 30;

        private final List<byte[]> fieldValuePairs;
        private final HashSeed hashSeed;
        private final int[] pairIndexes;

        private FieldPairIndex(List<byte[]> fieldValuePairs, int additionalPairs, HashSeed hashSeed) {
            this.fieldValuePairs = Objects.requireNonNull(fieldValuePairs, "fieldValuePairs");
            this.hashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
            long expectedPairs = (long) fieldValuePairs.size() / 2L + additionalPairs;
            this.pairIndexes = new int[indexCapacity(expectedPairs)];
            for (int pairIndex = 0; pairIndex < fieldValuePairs.size(); pairIndex += 2) {
                add(pairIndex);
            }
        }

        private int find(byte[] field) {
            int slot = slot(field);
            int mask = pairIndexes.length - 1;
            while (true) {
                int encodedPairIndex = pairIndexes[slot];
                if (encodedPairIndex == 0) {
                    return -1;
                }
                int pairIndex = encodedPairIndex - 1;
                if (Arrays.equals(fieldValuePairs.get(pairIndex), field)) {
                    return pairIndex;
                }
                slot = (slot + 1) & mask;
            }
        }

        private void add(int pairIndex) {
            byte[] field = fieldValuePairs.get(pairIndex);
            int slot = slot(field);
            int mask = pairIndexes.length - 1;
            while (pairIndexes[slot] != 0) {
                int existingPairIndex = pairIndexes[slot] - 1;
                if (Arrays.equals(fieldValuePairs.get(existingPairIndex), field)) {
                    throw new IllegalStateException("hash source contains duplicate fields");
                }
                slot = (slot + 1) & mask;
            }
            pairIndexes[slot] = pairIndex + 1;
        }

        private int slot(byte[] field) {
            int hash = SipHash24.foldToInt(SipHash24.hash(hashSeed, field));
            hash ^= hash >>> 16;
            return hash & (pairIndexes.length - 1);
        }

        private static int indexCapacity(long expectedPairs) {
            long required = Math.max(16L, expectedPairs * 2L);
            if (required > MAX_CAPACITY) {
                throw new IllegalArgumentException("too many hash fields to stage");
            }
            int capacity = 16;
            while (capacity < required) {
                capacity <<= 1;
            }
            return capacity;
        }
    }
}
