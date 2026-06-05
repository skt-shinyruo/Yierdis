package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.memory.internal.value.ListValue;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public final class ListRoot implements TypeRoot {
    private final NativeCollectionRootTable<ListValue> lists;
    private boolean closed;

    public ListRoot(NativeAllocator allocator) {
        this.lists = new NativeCollectionRootTable<>(
                Objects.requireNonNull(allocator, "allocator"),
                NativeObjectKind.LIST_ROOT,
                "list",
                false
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
        return lists.create(rootHandle -> newListValue(rootHandle));
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

    public synchronized void forEachNativeHandle(ValueHandle handle, Consumer<NativeHandle> consumer) {
        ensureOpen();
        requireList(handle).forEachNativeHandle(consumer);
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

    private ListValue newListValue(NativeHandle rootHandle) {
        return new ListValue(lists.allocator(), rootHandle);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("list root is closed");
        }
    }
}
