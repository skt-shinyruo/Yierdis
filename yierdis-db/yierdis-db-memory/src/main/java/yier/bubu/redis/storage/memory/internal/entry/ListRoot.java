package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.memory.internal.value.ListValue;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.List;
import java.util.Objects;

public final class ListRoot implements TypeRoot {
    private final YierdisFfmMemoryRuntime runtime;
    private final NativeCollectionRootTable<ListValue> lists;
    private boolean closed;

    public ListRoot(YierdisFfmMemoryRuntime runtime) {
        this(runtime, new YierdisStableNativeAllocator(Objects.requireNonNull(runtime, "runtime"), 4096), true);
    }

    public ListRoot(YierdisFfmMemoryRuntime runtime, NativeAllocator allocator) {
        this(runtime, allocator, false);
    }

    public ListRoot(NativeAllocator allocator) {
        this(null, allocator, false);
    }

    private ListRoot(YierdisFfmMemoryRuntime runtime, NativeAllocator allocator, boolean ownsAllocator) {
        this.runtime = runtime;
        this.lists = new NativeCollectionRootTable<>(
                allocator,
                NativeObjectKind.LIST_NODE,
                "list",
                ownsAllocator
        );
    }

    NativeAllocator allocator() {
        return lists.allocator();
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
        return lists.contains(handle);
    }

    public synchronized ValueHandle create() {
        ensureOpen();
        return lists.create(this::newListValue);
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
        return lists.adapterBytes(ListValue::estimatedBytes);
    }

    @Override
    public synchronized void release(ValueHandle handle) {
        lists.release(handle);
    }

    @Override
    public synchronized void clear() {
        ensureOpen();
        lists.clear();
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        lists.close();
        closed = true;
    }

    private ListValue requireList(ValueHandle handle) {
        return lists.require(handle);
    }

    private ListValue newListValue() {
        return runtime == null ? new ListValue() : new ListValue(runtime);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("list root is closed");
        }
    }
}
