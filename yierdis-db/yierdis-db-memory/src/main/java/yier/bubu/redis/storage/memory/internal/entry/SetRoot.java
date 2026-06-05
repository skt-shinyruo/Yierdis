package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.memory.internal.value.SetValue;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public final class SetRoot implements TypeRoot {
    private final NativeCollectionRootTable<SetValue> sets;
    private boolean closed;

    public SetRoot(NativeAllocator allocator) {
        this.sets = new NativeCollectionRootTable<>(
                Objects.requireNonNull(allocator, "allocator"),
                NativeObjectKind.SET_ROOT,
                "set",
                false
        );
    }

    NativeAllocator allocator() {
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
        return sets.adapterBytes(SetValue::estimatedBytes);
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
        return new SetValue(sets.allocator());
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("set root is closed");
        }
    }
}
