package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.memory.internal.value.HashValue;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.List;
import java.util.Objects;

public final class HashRoot implements TypeRoot {
    private final YierdisFfmMemoryRuntime runtime;
    private final NativeCollectionRootTable<HashValue> hashes;
    private boolean closed;

    public HashRoot(YierdisFfmMemoryRuntime runtime) {
        this(runtime, new YierdisStableNativeAllocator(Objects.requireNonNull(runtime, "runtime"), 4096), true);
    }

    public HashRoot(YierdisFfmMemoryRuntime runtime, NativeAllocator allocator) {
        this(runtime, allocator, false);
    }

    public HashRoot(NativeAllocator allocator) {
        this(null, allocator, false);
    }

    private HashRoot(YierdisFfmMemoryRuntime runtime, NativeAllocator allocator, boolean ownsAllocator) {
        this.runtime = runtime;
        this.hashes = new NativeCollectionRootTable<>(
                allocator,
                NativeObjectKind.HASH_NODE,
                "hash",
                ownsAllocator
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
            requireHash(handle).hsetMany(value.hgetallPairs());
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
        return requireHash(handle).hsetMany(fieldValuePairs);
    }

    public synchronized int hset(ValueHandle handle, byte[] field, byte[] value) {
        ensureOpen();
        return requireHash(handle).hset(field, value);
    }

    public synchronized byte[] hget(ValueHandle handle, byte[] field) {
        ensureOpen();
        return requireHash(handle).hget(field);
    }

    public synchronized int hdel(ValueHandle handle, List<byte[]> fields) {
        ensureOpen();
        return requireHash(handle).hdel(fields);
    }

    public synchronized int hgetallCount(ValueHandle handle) {
        ensureOpen();
        return requireHash(handle).hgetallCount();
    }

    public synchronized void hgetallPairsInto(ValueHandle handle, BulkStringSink out) {
        ensureOpen();
        requireHash(handle).hgetallPairsInto(out);
    }

    public synchronized int size(ValueHandle handle) {
        ensureOpen();
        return requireHash(handle).size();
    }

    @Override
    public synchronized long estimatedBytes(ValueHandle handle) {
        ensureOpen();
        return requireHash(handle).estimatedBytes();
    }

    public synchronized long nativeBytes() {
        return hashes.adapterBytes(HashValue::estimatedBytes);
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
        return runtime == null ? new HashValue() : new HashValue(runtime);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("hash root is closed");
        }
    }
}
