package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.memory.internal.value.ListValue;
import yier.bubu.redis.storage.memory.internal.value.NativeListEntryRef;
import yier.bubu.redis.storage.memory.internal.value.PreparedPoppedValueSequence;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public final class ListRoot implements TypeRoot {
    private final NativeCollectionRootTable<ListValue> lists;
    private boolean closed;

    public ListRoot(StableMemoryBackend allocator) {
        this.lists = new NativeCollectionRootTable<>(
                Objects.requireNonNull(allocator, "allocator"),
                NativeObjectKind.LIST_ROOT,
                "list",
                false
        );
    }

    StableMemoryBackend allocator() {
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

    public synchronized ValueHandle build(List<byte[]> orderedValues) {
        ensureOpen();
        Objects.requireNonNull(orderedValues, "orderedValues");
        ValueHandle handle = create();
        boolean built = false;
        try {
            ListValue value = requireList(handle);
            try {
                value.loadForBuild(orderedValues);
            } finally {
                lists.refreshAdapter(handle);
            }
            built = true;
            return handle;
        } finally {
            if (!built) {
                release(handle);
            }
        }
    }

    public synchronized ValueHandle store(ListValue value) {
        ensureOpen();
        Objects.requireNonNull(value, "value");
        ValueHandle handle = create();
        ListValue stored = requireList(handle);
        boolean ok = false;
        try {
            rpush(handle, value.range(0, -1));
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
        ListValue value = requireList(handle);
        try {
            value.lpushAll(values);
        } finally {
            lists.refreshAdapter(handle);
        }
    }

    public synchronized void rpush(ValueHandle handle, List<byte[]> values) {
        ensureOpen();
        if (values == null || values.isEmpty()) {
            return;
        }
        ListValue value = requireList(handle);
        try {
            value.rpushAll(values);
        } finally {
            lists.refreshAdapter(handle);
        }
    }

    public synchronized List<byte[]> lpop(ValueHandle handle, int count) {
        ensureOpen();
        ListValue value = requireList(handle);
        try {
            return value.lpop(count);
        } finally {
            lists.refreshAdapter(handle);
        }
    }

    public synchronized List<byte[]> rpop(ValueHandle handle, int count) {
        ensureOpen();
        ListValue value = requireList(handle);
        try {
            return value.rpop(count);
        } finally {
            lists.refreshAdapter(handle);
        }
    }

    public synchronized List<byte[]> range(ValueHandle handle, int start, int stop) {
        ensureOpen();
        return requireList(handle).range(start, stop);
    }

    public synchronized int rangeCount(ValueHandle handle, int start, int stop) {
        ensureOpen();
        return requireList(handle).rangeCount(start, stop);
    }

    public synchronized void rangeInto(ValueHandle handle, int start, int stop, ByteValueSink out) {
        ensureOpen();
        requireList(handle).rangeInto(start, stop, out);
    }

    public synchronized void emitPopRange(ValueHandle handle, int count, boolean left, ByteValueSink out) {
        ensureOpen();
        requireList(handle).emitPopRange(count, left, out);
    }

    public synchronized NativeListEntryRef[] popEntries(ValueHandle handle, int count, boolean left) {
        ensureOpen();
        return requireList(handle).popEntries(count, left);
    }

    public synchronized long retainedBytes(ValueHandle handle) {
        ensureOpen();
        return NativeStorageLayout.COLLECTION_ROOT_RECORD_BYTES + requireList(handle).estimatedBytes();
    }

    public synchronized int[] nativePayloadSizes(ValueHandle handle) {
        ensureOpen();
        return requireList(handle).nativePayloadSizes();
    }

    public synchronized int size(ValueHandle handle) {
        ensureOpen();
        return requireList(handle).size();
    }

    public synchronized ListValue.PreparedMutation preparePush(
            ValueHandle source,
            List<byte[]> values,
            boolean left
    ) {
        ensureOpen();
        return requireList(source).preparePush(values, left);
    }

    public synchronized ListValue.PreparedMutation preparePop(
            ValueHandle source,
            int count,
            boolean left
    ) {
        ensureOpen();
        return requireList(source).preparePop(count, left);
    }

    public synchronized long estimatedPreparedPushHeapGrowthBytes(
            ValueHandle source,
            List<byte[]> values,
            boolean left,
            int expectedNativeAllocationCount
    ) {
        ensureOpen();
        Objects.requireNonNull(values, "values");
        if (source != null) {
            return requireList(source).preparedPushHeapUpperBound(values, left);
        }
        long replacementHeapBytes = ListValue.preparedNewHeapUpperBound(values);
        return lists.estimatedNewAdapterHeapGrowthBytes(replacementHeapBytes, expectedNativeAllocationCount);
    }

    public synchronized int[] preparedPushNativeAllocationSizes(
            ValueHandle source,
            List<byte[]> values,
            boolean left
    ) {
        ensureOpen();
        Objects.requireNonNull(values, "values");
        return source == null
                ? ListValue.preparedNewNativeAllocationSizes(values, left)
                : requireList(source).preparedPushNativeAllocationSizes(values, left);
    }

    public synchronized int[] preparedPopNativeAllocationSizes(ValueHandle source, int count, boolean left) {
        ensureOpen();
        return requireList(source).preparedPopNativeAllocationSizes(count, left);
    }

    public synchronized long estimatedPreparedPopHeapGrowthBytes(ValueHandle source, int count, boolean left) {
        ensureOpen();
        return requireList(source).preparedPopHeapUpperBound(count, left);
    }

    public synchronized long retainedHeapBytes() {
        ensureOpen();
        return lists.heapBytes();
    }

    public synchronized long positiveRetainedHeapGrowthBytes(long before) {
        long after = retainedHeapBytes();
        return after > before ? after - before : 0L;
    }

    @Override
    public synchronized long estimatedBytes(ValueHandle handle) {
        ensureOpen();
        return requireList(handle).estimatedBytes();
    }

    public synchronized long nativeBytes() {
        return lists.adapterBytes(ListValue::estimatedBytes);
    }

    public synchronized long heapBytes() {
        ensureOpen();
        return lists.heapBytes();
    }

    public synchronized void armIterationTrapForTesting() {
        lists.armIterationTrapForTesting();
    }

    public synchronized void disarmIterationTrapForTesting() {
        lists.disarmIterationTrapForTesting();
    }

    public synchronized void forEachNativeHandle(ValueHandle handle, Consumer<NativeHandle> consumer) {
        ensureOpen();
        requireList(handle).forEachNativeHandle(consumer);
    }

    @Override
    public synchronized void release(ValueHandle handle) {
        lists.release(handle);
    }

    public synchronized void releaseExcept(ValueHandle handle, PreparedPoppedValueSequence retained) {
        ensureOpen();
        if (handle == null) {
            return;
        }
        ListValue value = requireList(handle);
        value.releaseExcept(retained);
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
