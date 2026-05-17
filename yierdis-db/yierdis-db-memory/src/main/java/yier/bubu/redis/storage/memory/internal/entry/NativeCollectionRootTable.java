package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.StaleNativeHandleException;
import yier.bubu.redis.storage.memory.internal.value.YierdisValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

final class NativeCollectionRootTable<T extends YierdisValue> {
    private static final int ROOT_RECORD_BYTES = Long.BYTES;

    private final NativeAllocator allocator;
    private final NativeObjectKind kind;
    private final String label;
    private final boolean ownsAllocator;
    private final Map<Long, T> adapters = new HashMap<>();

    NativeCollectionRootTable(
            NativeAllocator allocator,
            NativeObjectKind kind,
            String label,
            boolean ownsAllocator
    ) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.label = Objects.requireNonNull(label, "label");
        this.ownsAllocator = ownsAllocator;
    }

    NativeAllocator allocator() {
        return allocator;
    }

    ValueHandle create(Supplier<? extends T> adapterFactory) {
        Objects.requireNonNull(adapterFactory, "adapterFactory");
        return create(ignored -> adapterFactory.get());
    }

    ValueHandle create(Function<NativeHandle, ? extends T> adapterFactory) {
        Objects.requireNonNull(adapterFactory, "adapterFactory");
        NativeHandle nativeHandle = allocator.allocate(kind, ROOT_RECORD_BYTES);
        T adapter = null;
        try {
            writeRootRecord(nativeHandle);
            adapter = adapterFactory.apply(nativeHandle);
            adapters.put(nativeHandle.raw(), adapter);
            return ValueHandle.fromNativeHandle(nativeHandle);
        } catch (RuntimeException | Error e) {
            if (adapter != null) {
                try {
                    adapter.close();
                } catch (RuntimeException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
            }
            try {
                allocator.free(nativeHandle);
            } catch (RuntimeException freeFailure) {
                e.addSuppressed(freeFailure);
            }
            throw e;
        }
    }

    boolean contains(ValueHandle handle) {
        NativeHandle nativeHandle = nativeHandleOrNull(handle);
        if (nativeHandle == null || !adapters.containsKey(nativeHandle.raw())) {
            return false;
        }
        try {
            validateRootRecord(nativeHandle);
            return true;
        } catch (StaleNativeHandleException e) {
            return false;
        }
    }

    T require(ValueHandle handle) {
        NativeHandle nativeHandle = requireNativeHandle(handle);
        validateRootRecord(nativeHandle);
        T adapter = adapters.get(nativeHandle.raw());
        if (adapter == null) {
            throw new IllegalArgumentException("unknown " + label + " value handle: " + handle.raw());
        }
        return adapter;
    }

    long adapterBytes(ToLongFunction<? super T> estimator) {
        Objects.requireNonNull(estimator, "estimator");
        long total = 0L;
        for (T adapter : adapters.values()) {
            total = addSaturating(total, estimator.applyAsLong(adapter));
        }
        return total;
    }

    void release(ValueHandle handle) {
        if (handle == null) {
            return;
        }
        NativeHandle nativeHandle = requireNativeHandle(handle);
        T adapter = adapters.get(nativeHandle.raw());
        if (adapter == null) {
            return;
        }
        RuntimeException failure = null;
        boolean adapterClosed = false;
        boolean rootFreed = false;
        try {
            adapter.close();
            adapterClosed = true;
        } catch (RuntimeException e) {
            failure = addFailure(failure, e);
        }
        try {
            allocator.free(nativeHandle);
            rootFreed = true;
        } catch (StaleNativeHandleException ignored) {
            // The allocator, not the adapter table, owns liveness. External free leaves the adapter stale.
            rootFreed = true;
        } catch (RuntimeException e) {
            failure = addFailure(failure, e);
        }
        if (adapterClosed && rootFreed) {
            adapters.remove(nativeHandle.raw());
        }
        if (failure != null) {
            throw failure;
        }
    }

    void clear() {
        RuntimeException failure = null;
        Long[] handles = adapters.keySet().toArray(Long[]::new);
        for (long raw : handles) {
            try {
                release(ValueHandle.fromRaw(raw));
            } catch (RuntimeException e) {
                failure = addFailure(failure, e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    void close() {
        RuntimeException failure = null;
        try {
            clear();
        } catch (RuntimeException e) {
            failure = addFailure(failure, e);
        }
        if (ownsAllocator) {
            try {
                allocator.close();
            } catch (RuntimeException e) {
                failure = addFailure(failure, e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void writeRootRecord(NativeHandle handle) {
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
            setLong(view, 0, handle.raw());
        }
    }

    private void validateRootRecord(NativeHandle handle) {
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
            if (view.size() != ROOT_RECORD_BYTES) {
                throw new IllegalStateException(label + " root record size mismatch: " + handle.raw());
            }
            long storedHandle = getLong(view, 0);
            if (storedHandle != handle.raw()) {
                throw new IllegalStateException(label + " root record handle mismatch: " + handle.raw());
            }
        }
    }

    private NativeHandle requireNativeHandle(ValueHandle handle) {
        NativeHandle nativeHandle = nativeHandleOrNull(handle);
        if (nativeHandle == null) {
            throw new IllegalArgumentException("value handle is not " + label + " root: "
                    + (handle == null ? "null" : handle.raw()));
        }
        return nativeHandle;
    }

    private NativeHandle nativeHandleOrNull(ValueHandle handle) {
        if (handle == null || handle.isNull()) {
            return null;
        }
        NativeHandle nativeHandle = handle.nativeHandle();
        if (nativeHandle.domain() != kind.domain() || nativeHandle.kindCode() != kind.code()) {
            return null;
        }
        return nativeHandle;
    }

    private static long getLong(NativeObjectView view, int offset) {
        return ((long) view.getByte(offset) & 0xff)
                | (((long) view.getByte(offset + 1) & 0xff) << 8)
                | (((long) view.getByte(offset + 2) & 0xff) << 16)
                | (((long) view.getByte(offset + 3) & 0xff) << 24)
                | (((long) view.getByte(offset + 4) & 0xff) << 32)
                | (((long) view.getByte(offset + 5) & 0xff) << 40)
                | (((long) view.getByte(offset + 6) & 0xff) << 48)
                | (((long) view.getByte(offset + 7) & 0xff) << 56);
    }

    private static void setLong(NativeObjectView view, int offset, long value) {
        for (int i = 0; i < Long.BYTES; i++) {
            view.setByte(offset + i, (byte) (value >>> (i * 8)));
        }
    }

    private static long addSaturating(long left, long right) {
        if (left < 0 || right < 0 || Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static RuntimeException addFailure(RuntimeException failure, RuntimeException next) {
        if (failure == null) {
            return next;
        }
        failure.addSuppressed(next);
        return failure;
    }
}
