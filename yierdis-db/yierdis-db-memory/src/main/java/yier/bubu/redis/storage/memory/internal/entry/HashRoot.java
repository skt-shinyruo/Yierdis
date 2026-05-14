package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.memory.internal.value.HashValue;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class HashRoot implements TypeRoot {
    private final YierdisFfmMemoryRuntime runtime;
    private final Map<Long, HashValue> hashes = new HashMap<>();
    private long nextHandle = 1L;
    private boolean closed;

    public HashRoot(YierdisFfmMemoryRuntime runtime) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
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
        return handle != null && hashes.containsKey(handle.raw());
    }

    public synchronized ValueHandle create() {
        ensureOpen();
        ValueHandle handle = newHandle();
        hashes.put(handle.raw(), new HashValue(runtime));
        return handle;
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
        long total = 0L;
        for (HashValue hash : hashes.values()) {
            total = addSaturating(total, hash.estimatedBytes());
        }
        return total;
    }

    @Override
    public synchronized void release(ValueHandle handle) {
        if (handle == null) {
            return;
        }
        HashValue removed = hashes.remove(handle.raw());
        if (removed != null) {
            removed.close();
        }
    }

    @Override
    public synchronized void clear() {
        ensureOpen();
        RuntimeException failure = null;
        for (HashValue hash : hashes.values()) {
            try {
                hash.close();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        hashes.clear();
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        clear();
        closed = true;
    }

    private HashValue requireHash(ValueHandle handle) {
        Objects.requireNonNull(handle, "handle");
        HashValue hash = hashes.get(handle.raw());
        if (hash == null) {
            throw new IllegalArgumentException("unknown hash value handle: " + handle.raw());
        }
        return hash;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("hash root is closed");
        }
    }

    private ValueHandle newHandle() {
        NativeObjectKind kind = NativeObjectKind.HASH_NODE;
        NativeHandle handle = NativeHandle.of(kind.domain(), kind, nextHandle++, 1, 0);
        return ValueHandle.fromNativeHandle(handle);
    }

    private static long addSaturating(long left, long right) {
        if (left < 0 || right < 0 || Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }
}
