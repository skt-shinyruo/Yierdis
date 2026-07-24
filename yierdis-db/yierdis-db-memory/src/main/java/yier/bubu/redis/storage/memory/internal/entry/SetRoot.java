package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.value.SetValue;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public final class SetRoot implements TypeRoot {
    private final NativeCollectionRootTable<SetValue> sets;
    private final HashSeed hashSeed;
    private final HashTableMaintenanceRegistry maintenanceRegistry;
    private boolean closed;

    public SetRoot(StableMemoryBackend allocator) {
        this(allocator, HashSeed.random());
    }

    public SetRoot(StableMemoryBackend allocator, HashSeed hashSeed) {
        this(allocator, hashSeed, null);
    }

    public SetRoot(
            StableMemoryBackend allocator,
            HashSeed hashSeed,
            HashTableMaintenanceRegistry maintenanceRegistry
    ) {
        this.hashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
        this.maintenanceRegistry = maintenanceRegistry;
        this.sets = new NativeCollectionRootTable<>(
                Objects.requireNonNull(allocator, "allocator"),
                NativeObjectKind.SET_ROOT,
                "set",
                false
        );
    }

    StableMemoryBackend allocator() {
        return sets.allocator();
    }

    @Override
    public ValueType type() {
        return ValueType.SET;
    }

    @Override
    public ValueEncoding encoding() {
        return ValueEncoding.SET_INTSET;
    }

    public synchronized ValueEncoding encoding(ValueHandle handle) {
        return requireSet(handle).encoding();
    }

    public synchronized boolean contains(ValueHandle handle) {
        ensureOpen();
        return sets.contains(handle);
    }

    public synchronized ValueHandle create() {
        ensureOpen();
        return sets.create(this::newSetValue);
    }

    public synchronized ValueHandle store(SetValue value) {
        ensureOpen();
        Objects.requireNonNull(value, "value");
        ValueHandle handle = create();
        boolean ok = false;
        try {
            sadd(handle, value.members());
            ok = true;
            return handle;
        } finally {
            if (!ok) {
                release(handle);
            }
        }
    }

    public synchronized int sadd(ValueHandle handle, List<byte[]> members) {
        ensureOpen();
        SetValue set = requireSet(handle);
        try {
            return set.addAll(members);
        } finally {
            sets.refreshAdapter(handle);
        }
    }

    public synchronized int srem(ValueHandle handle, List<byte[]> members) {
        ensureOpen();
        SetValue set = requireSet(handle);
        try {
            return set.removeAll(members);
        } finally {
            sets.refreshAdapter(handle);
        }
    }

    public synchronized int countAdditions(ValueHandle handle, List<byte[]> members) {
        ensureOpen();
        return requireSet(handle).countAdditions(members);
    }

    public synchronized int countExistingMembers(ValueHandle handle, List<byte[]> members) {
        ensureOpen();
        return requireSet(handle).countExistingMembers(members);
    }

    public synchronized List<byte[]> members(ValueHandle handle) {
        ensureOpen();
        return requireSet(handle).members();
    }

    public synchronized int[] nativePayloadSizes(ValueHandle handle) {
        ensureOpen();
        return requireSet(handle).nativePayloadSizes();
    }

    public synchronized boolean contains(ValueHandle handle, byte[] member) {
        ensureOpen();
        return requireSet(handle).contains(member);
    }

    public synchronized int size(ValueHandle handle) {
        ensureOpen();
        return requireSet(handle).size();
    }

    public synchronized long estimatedPreparedAddHeapGrowthBytes(
            ValueHandle source,
            List<byte[]> members,
            int expectedNativeAllocationCount
    ) {
        ensureOpen();
        Objects.requireNonNull(members, "members");
        long replacementHeapBytes = source == null
                ? SetValue.preparedNewHeapUpperBound(members)
                : requireSet(source).preparedCopyHeapUpperBound(members);
        return sets.estimatedNewAdapterHeapGrowthBytes(replacementHeapBytes, expectedNativeAllocationCount);
    }

    public synchronized long retainedHeapBytes() {
        ensureOpen();
        long registryHeapBytes = maintenanceRegistry == null ? 0L : maintenanceRegistry.heapEstimatedBytes();
        return addSaturating(sets.heapBytes(), registryHeapBytes);
    }

    public synchronized long positiveRetainedHeapGrowthBytes(long before) {
        return positiveDelta(retainedHeapBytes(), before);
    }

    public synchronized void membersInto(ValueHandle handle, ByteValueSink out) {
        ensureOpen();
        requireSet(handle).membersInto(out);
    }

    public synchronized CollectionScanWindow sscan(
            ValueHandle handle,
            ScanCursorV2 cursor,
            byte[] globPattern,
            int count
    ) {
        ensureOpen();
        return requireSet(handle).sscan(cursor, globPattern, count);
    }

    @Override
    public synchronized long estimatedBytes(ValueHandle handle) {
        ensureOpen();
        return requireSet(handle).estimatedBytes();
    }

    public synchronized long nativeBytes() {
        return sets.adapterBytes(SetValue::estimatedBytes);
    }

    public synchronized long heapBytes() {
        ensureOpen();
        return sets.heapBytes();
    }

    public synchronized void armIterationTrapForTesting() {
        sets.armIterationTrapForTesting();
    }

    public synchronized void disarmIterationTrapForTesting() {
        sets.disarmIterationTrapForTesting();
    }

    public synchronized void forEachNativeHandle(ValueHandle handle, Consumer<NativeHandle> consumer) {
        ensureOpen();
        requireSet(handle).forEachNativeHandle(consumer);
    }

    @Override
    public synchronized void release(ValueHandle handle) {
        sets.release(handle);
    }

    @Override
    public synchronized void clear() {
        ensureOpen();
        sets.clear();
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        sets.close();
        closed = true;
    }

    private SetValue requireSet(ValueHandle handle) {
        return sets.require(handle);
    }

    private SetValue newSetValue() {
        return new SetValue(sets.allocator(), hashSeed, maintenanceRegistry);
    }

    private static long positiveDelta(long after, long before) {
        return after > before ? after - before : 0L;
    }

    private static long addSaturating(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("set root is closed");
        }
    }
}
