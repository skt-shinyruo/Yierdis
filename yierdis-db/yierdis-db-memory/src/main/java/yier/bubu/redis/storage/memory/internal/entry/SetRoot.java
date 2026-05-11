package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.memory.internal.value.SetValue;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class SetRoot implements TypeRoot {
    private final YierdisFfmMemoryRuntime runtime;
    private final Map<Long, SetValue> sets = new HashMap<>();
    private long nextHandle = 1L;
    private boolean closed;

    public SetRoot(YierdisFfmMemoryRuntime runtime) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
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
        return handle != null && sets.containsKey(handle.raw());
    }

    public synchronized ValueHandle create() {
        ensureOpen();
        ValueHandle handle = new ValueHandle(nextHandle++);
        sets.put(handle.raw(), new SetValue(runtime));
        return handle;
    }

    public synchronized ValueHandle store(SetValue value) {
        ensureOpen();
        Objects.requireNonNull(value, "value");
        ValueHandle handle = create();
        boolean ok = false;
        try {
            requireSet(handle).addAll(value.members());
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
        return requireSet(handle).addAll(members);
    }

    public synchronized int srem(ValueHandle handle, List<byte[]> members) {
        ensureOpen();
        return requireSet(handle).removeAll(members);
    }

    public synchronized boolean contains(ValueHandle handle, byte[] member) {
        ensureOpen();
        return requireSet(handle).contains(member);
    }

    public synchronized int size(ValueHandle handle) {
        ensureOpen();
        return requireSet(handle).size();
    }

    public synchronized void membersInto(ValueHandle handle, BulkStringSink out) {
        ensureOpen();
        requireSet(handle).membersInto(out);
    }

    @Override
    public synchronized long estimatedBytes(ValueHandle handle) {
        ensureOpen();
        return requireSet(handle).estimatedBytes();
    }

    public synchronized long nativeBytes() {
        long total = 0L;
        for (SetValue set : sets.values()) {
            total = addSaturating(total, set.estimatedBytes());
        }
        return total;
    }

    @Override
    public synchronized void release(ValueHandle handle) {
        if (handle == null) {
            return;
        }
        SetValue removed = sets.remove(handle.raw());
        if (removed != null) {
            removed.close();
        }
    }

    @Override
    public synchronized void clear() {
        ensureOpen();
        RuntimeException failure = null;
        for (SetValue set : sets.values()) {
            try {
                set.close();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        sets.clear();
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

    private SetValue requireSet(ValueHandle handle) {
        Objects.requireNonNull(handle, "handle");
        SetValue set = sets.get(handle.raw());
        if (set == null) {
            throw new IllegalArgumentException("unknown set value handle: " + handle.raw());
        }
        return set;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("set root is closed");
        }
    }

    private static long addSaturating(long left, long right) {
        if (left < 0 || right < 0 || Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }
}
