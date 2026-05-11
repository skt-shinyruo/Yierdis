package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.memory.internal.value.ListValue;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class ListRoot implements TypeRoot {
    private final YierdisFfmMemoryRuntime runtime;
    private final Map<Long, ListValue> lists = new HashMap<>();
    private long nextHandle = 1L;
    private boolean closed;

    public ListRoot(YierdisFfmMemoryRuntime runtime) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
    }

    @Override
    public ValueType type() {
        return ValueType.LIST;
    }

    @Override
    public ValueEncoding encoding() {
        return ValueEncoding.LIST_PACKED;
    }

    public synchronized ValueEncoding encoding(ValueHandle handle) {
        return requireList(handle).encoding();
    }

    public synchronized boolean contains(ValueHandle handle) {
        ensureOpen();
        return handle != null && lists.containsKey(handle.raw());
    }

    public synchronized ValueHandle create() {
        ensureOpen();
        ValueHandle handle = new ValueHandle(nextHandle++);
        lists.put(handle.raw(), new ListValue(runtime));
        return handle;
    }

    public synchronized ValueHandle store(ListValue value) {
        ensureOpen();
        Objects.requireNonNull(value, "value");
        ValueHandle handle = create();
        ListValue stored = requireList(handle);
        boolean ok = false;
        try {
            stored.rpushAll(value.range(0, -1));
            ok = true;
            return handle;
        } finally {
            if (!ok) {
                release(handle);
            }
        }
    }

    public synchronized void lpush(ValueHandle handle, List<byte[]> values) {
        ensureOpen();
        if (values == null || values.isEmpty()) {
            return;
        }
        requireList(handle).lpushAll(values);
    }

    public synchronized void rpush(ValueHandle handle, List<byte[]> values) {
        ensureOpen();
        if (values == null || values.isEmpty()) {
            return;
        }
        requireList(handle).rpushAll(values);
    }

    public synchronized List<byte[]> lpop(ValueHandle handle, int count) {
        ensureOpen();
        return requireList(handle).lpop(count);
    }

    public synchronized List<byte[]> rpop(ValueHandle handle, int count) {
        ensureOpen();
        return requireList(handle).rpop(count);
    }

    public synchronized int rangeCount(ValueHandle handle, int start, int stop) {
        ensureOpen();
        return requireList(handle).rangeCount(start, stop);
    }

    public synchronized void rangeInto(ValueHandle handle, int start, int stop, BulkStringSink out) {
        ensureOpen();
        requireList(handle).rangeInto(start, stop, out);
    }

    public synchronized int size(ValueHandle handle) {
        ensureOpen();
        return requireList(handle).size();
    }

    @Override
    public synchronized long estimatedBytes(ValueHandle handle) {
        ensureOpen();
        return requireList(handle).estimatedBytes();
    }

    public synchronized long nativeBytes() {
        long total = 0L;
        for (ListValue list : lists.values()) {
            long bytes = list.estimatedBytes();
            if (bytes < 0 || Long.MAX_VALUE - total < bytes) {
                return Long.MAX_VALUE;
            }
            total += bytes;
        }
        return total;
    }

    @Override
    public synchronized void release(ValueHandle handle) {
        if (handle == null) {
            return;
        }
        ListValue removed = lists.remove(handle.raw());
        if (removed != null) {
            removed.close();
        }
    }

    @Override
    public synchronized void clear() {
        ensureOpen();
        RuntimeException failure = null;
        for (ListValue list : lists.values()) {
            try {
                list.close();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        lists.clear();
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

    private ListValue requireList(ValueHandle handle) {
        Objects.requireNonNull(handle, "handle");
        ListValue list = lists.get(handle.raw());
        if (list == null) {
            throw new IllegalArgumentException("unknown list value handle: " + handle.raw());
        }
        return list;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("list root is closed");
        }
    }
}
