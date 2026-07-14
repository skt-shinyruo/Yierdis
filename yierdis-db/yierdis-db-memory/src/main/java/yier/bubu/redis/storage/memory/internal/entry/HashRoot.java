package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.api.result.BulkStringValue;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.value.HashValue;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public final class HashRoot implements TypeRoot {
    private final NativeCollectionRootTable<HashValue> hashes;
    private final HashSeed hashSeed;
    private final HashTableMaintenanceRegistry maintenanceRegistry;
    private boolean closed;

    public HashRoot(NativeAllocator allocator) {
        this(allocator, HashSeed.random());
    }

    public HashRoot(NativeAllocator allocator, HashSeed hashSeed) {
        this(allocator, hashSeed, null);
    }

    public HashRoot(
            NativeAllocator allocator,
            HashSeed hashSeed,
            HashTableMaintenanceRegistry maintenanceRegistry
    ) {
        this.hashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
        this.maintenanceRegistry = maintenanceRegistry;
        this.hashes = new NativeCollectionRootTable<>(
                Objects.requireNonNull(allocator, "allocator"),
                NativeObjectKind.HASH_ROOT,
                "hash",
                false
        );
    }

    NativeAllocator allocator() {
        return hashes.allocator();
    }

    @Override
    public ValueType type() {
        return ValueType.HASH;
    }

    @Override
    public ValueEncoding encoding() {
        return ValueEncoding.HASH_PACKED;
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
        return hashes.create(this::newHashValue);
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

    public synchronized byte[] hget(ValueHandle handle, byte[] field) {
        ensureOpen();
        return requireHash(handle).hget(field);
    }

    public synchronized BulkStringValue hgetValue(ValueHandle handle, byte[] field) {
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

    public synchronized void hgetallPairsInto(ValueHandle handle, BulkStringSink out) {
        ensureOpen();
        requireHash(handle).hgetallPairsInto(out);
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
            List<byte[]> fieldValuePairs,
            int expectedNativeAllocationCount
    ) {
        ensureOpen();
        Objects.requireNonNull(fieldValuePairs, "fieldValuePairs");
        long replacementHeapBytes = source == null
                ? HashValue.preparedNewHeapUpperBound(fieldValuePairs)
                : requireHash(source).preparedCopyHeapUpperBound(fieldValuePairs);
        return hashes.estimatedNewAdapterHeapGrowthBytes(replacementHeapBytes, expectedNativeAllocationCount);
    }

    public synchronized long retainedHeapBytes() {
        ensureOpen();
        long registryHeapBytes = maintenanceRegistry == null ? 0L : maintenanceRegistry.heapEstimatedBytes();
        return addSaturating(hashes.heapBytes(), registryHeapBytes);
    }

    public synchronized long positiveRetainedHeapGrowthBytes(long before) {
        return positiveDelta(retainedHeapBytes(), before);
    }

    @Override
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

    public synchronized void armIterationTrapForTesting() {
        hashes.armIterationTrapForTesting();
    }

    public synchronized void disarmIterationTrapForTesting() {
        hashes.disarmIterationTrapForTesting();
    }

    public synchronized void forEachNativeHandle(ValueHandle handle, Consumer<NativeHandle> consumer) {
        ensureOpen();
        requireHash(handle).forEachNativeHandle(consumer);
    }

    @Override
    public synchronized void release(ValueHandle handle) {
        hashes.release(handle);
    }

    @Override
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

    private static long positiveDelta(long after, long before) {
        return after > before ? after - before : 0L;
    }

    private static long addSaturating(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("hash root is closed");
        }
    }
}
