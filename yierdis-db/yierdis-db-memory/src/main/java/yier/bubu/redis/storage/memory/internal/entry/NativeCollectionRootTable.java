package yier.bubu.redis.storage.memory.internal.entry;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.StaleNativeHandleException;
import yier.bubu.redis.storage.memory.internal.value.HeapTrackedValue;
import yier.bubu.redis.storage.memory.internal.value.YierdisValue;

final class NativeCollectionRootTable<T extends YierdisValue> {
    private static final long MAP_OBJECT_BYTES = 48L;
    private static final long HASH_MAP_ENTRY_BYTES = 32L;
    private static final long ADAPTER_SLOT_HEAP_BYTES = 32L;
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long REFERENCE_BYTES = 8L;

    private final StableMemoryBackend allocator;
    private final NativeObjectKind kind;
    private final String label;
    private final boolean ownsAllocator;
    private Map<NativeHandle, AdapterSlot<T>> adapters = new HashMap<>();
    private int adapterMapCapacity;
    private long adapterMapHeapBytes = MAP_OBJECT_BYTES;
    private long adapterHeapBytes;
    private boolean iterationTrapForTesting;

    NativeCollectionRootTable(
            StableMemoryBackend allocator,
            NativeObjectKind kind,
            String label,
            boolean ownsAllocator
    ) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.label = Objects.requireNonNull(label, "label");
        this.ownsAllocator = ownsAllocator;
    }

    StableMemoryBackend allocator() {
        return allocator;
    }

    ValueHandle create(Supplier<? extends T> adapterFactory) {
        Objects.requireNonNull(adapterFactory, "adapterFactory");
        return create(ignored -> adapterFactory.get());
    }

    ValueHandle create(Function<NativeHandle, ? extends T> adapterFactory) {
        Objects.requireNonNull(adapterFactory, "adapterFactory");
        NativeHandle nativeHandle = allocator.allocate(
                kind,
                NativeStorageLayout.COLLECTION_ROOT_RECORD_BYTES
        );
        T adapter = null;
        try {
            writeRootRecord(nativeHandle);
            adapter = adapterFactory.apply(nativeHandle);
            if (adapter instanceof HeapTrackedValue heapTracked) {
                heapTracked.setHeapChangeListener(() -> refreshAdapter(nativeHandle));
            }
            putAdapter(nativeHandle, adapter);
            return new ValueHandle(nativeHandle);
        } catch (RuntimeException | Error failure) {
            if (adapter != null) {
                try {
                    adapter.close();
                } catch (RuntimeException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            try {
                allocator.free(nativeHandle);
            } catch (RuntimeException freeFailure) {
                failure.addSuppressed(freeFailure);
            }
            throw failure;
        }
    }

    boolean contains(ValueHandle handle) {
        NativeHandle nativeHandle = nativeHandleOrNull(handle);
        if (nativeHandle == null || adapterSlot(nativeHandle) == null) {
            return false;
        }
        try {
            validateRootRecord(nativeHandle);
            return true;
        } catch (StaleNativeHandleException ignored) {
            return false;
        }
    }

    T require(ValueHandle handle) {
        NativeHandle nativeHandle = requireNativeHandle(handle);
        AdapterSlot<T> slot = adapterSlot(nativeHandle);
        if (slot == null) {
            throw new IllegalArgumentException("unknown " + label + " value handle: " + nativeHandle);
        }
        validateRootRecord(nativeHandle);
        return slot.adapter;
    }

    long adapterBytes(ToLongFunction<? super T> estimator) {
        Objects.requireNonNull(estimator, "estimator");
        failIfIterationTrapped();
        long total = 0L;
        for (AdapterSlot<T> slot : adapters.values()) {
            total = addSaturating(total, estimator.applyAsLong(slot.adapter));
        }
        return total;
    }

    long heapBytes() {
        return addSaturating(adapterMapHeapBytes, adapterHeapBytes);
    }

    long estimatedNewAdapterHeapGrowthBytes(long adapterValueHeapBytes) {
        return estimatedNewAdapterHeapGrowthBytes(adapterValueHeapBytes, 1);
    }

    long estimatedNewAdapterHeapGrowthBytes(long adapterValueHeapBytes, int expectedNativeAllocationCount) {
        if (expectedNativeAllocationCount < 1) {
            throw new IllegalArgumentException("expectedNativeAllocationCount must be >= 1");
        }
        long growth = addSaturating(
                addSaturating(HASH_MAP_ENTRY_BYTES, ADAPTER_SLOT_HEAP_BYTES),
                nonNegative(adapterValueHeapBytes)
        );
        int nextCapacity = mapCapacityForEntries(adapters.size() + 1);
        if (nextCapacity > adapterMapCapacity) {
            growth = addSaturating(
                    growth,
                    subtractSaturating(hashMapTableHeapBytes(nextCapacity), hashMapTableHeapBytes(adapterMapCapacity))
            );
        }
        return growth;
    }

    void release(ValueHandle handle) {
        NativeHandle nativeHandle = nativeHandleOrNull(handle);
        if (nativeHandle == null) {
            return;
        }
        AdapterSlot<T> slot = adapterSlot(nativeHandle);
        if (slot == null) {
            return;
        }
        RuntimeException failure = null;
        boolean adapterClosed = false;
        boolean rootFreed = false;
        try {
            slot.adapter.close();
            adapterClosed = true;
        } catch (RuntimeException closeFailure) {
            failure = addFailure(failure, closeFailure);
        }
        try {
            allocator.free(nativeHandle);
            rootFreed = true;
        } catch (StaleNativeHandleException ignored) {
            // allocator 的存活性结论优先；外部释放 root 后仍可收敛残留适配器。
            rootFreed = true;
        } catch (RuntimeException freeFailure) {
            failure = addFailure(failure, freeFailure);
        }
        if (adapterClosed && rootFreed) {
            removeAdapter(nativeHandle, slot);
        }
        if (failure != null) {
            throw failure;
        }
    }

    void clear() {
        RuntimeException failure = null;
        // release 会移除 map 项，先快照完整句柄避免遍历期间改变集合结构。
        NativeHandle[] handles = adapters.keySet().toArray(NativeHandle[]::new);
        for (NativeHandle handle : handles) {
            try {
                release(new ValueHandle(handle));
            } catch (RuntimeException releaseFailure) {
                failure = addFailure(failure, releaseFailure);
            }
        }
        if (adapters.isEmpty()) {
            adapters = new HashMap<>();
            adapterMapCapacity = 0;
            adapterMapHeapBytes = MAP_OBJECT_BYTES;
        }
        if (failure != null) {
            throw failure;
        }
    }

    void refreshAdapter(ValueHandle handle) {
        NativeHandle nativeHandle = nativeHandleOrNull(handle);
        if (nativeHandle != null) {
            refreshAdapter(nativeHandle);
        }
    }

    void armIterationTrapForTesting() {
        iterationTrapForTesting = true;
    }

    void disarmIterationTrapForTesting() {
        iterationTrapForTesting = false;
    }

    private void putAdapter(NativeHandle handle, T adapter) {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(adapter, "adapter");
        if (adapters.containsKey(handle)) {
            throw new IllegalStateException(label + " adapter is already registered: " + handle);
        }
        int nextCapacity = mapCapacityForEntries(adapters.size() + 1);
        if (nextCapacity > adapterMapCapacity) {
            adapterMapHeapBytes = addSaturating(
                    adapterMapHeapBytes,
                    subtractSaturating(hashMapTableHeapBytes(nextCapacity), hashMapTableHeapBytes(adapterMapCapacity))
            );
            adapterMapCapacity = nextCapacity;
        }
        long adapterValueHeapBytes = heapEstimatedBytes(adapter);
        adapters.put(handle, new AdapterSlot<>(adapter, adapterValueHeapBytes));
        adapterHeapBytes = addSaturating(adapterHeapBytes, HASH_MAP_ENTRY_BYTES);
        adapterHeapBytes = addSaturating(adapterHeapBytes, ADAPTER_SLOT_HEAP_BYTES);
        adapterHeapBytes = addSaturating(adapterHeapBytes, adapterValueHeapBytes);
    }

    private AdapterSlot<T> adapterSlot(NativeHandle handle) {
        return adapters.get(handle);
    }

    private void removeAdapter(NativeHandle handle, AdapterSlot<T> expected) {
        if (!adapters.remove(handle, expected)) {
            return;
        }
        adapterHeapBytes = subtractSaturating(adapterHeapBytes, HASH_MAP_ENTRY_BYTES);
        adapterHeapBytes = subtractSaturating(adapterHeapBytes, ADAPTER_SLOT_HEAP_BYTES);
        adapterHeapBytes = subtractSaturating(adapterHeapBytes, expected.adapterHeapBytes);
    }

    private void refreshAdapter(NativeHandle handle) {
        AdapterSlot<T> slot = adapterSlot(handle);
        if (slot == null) {
            return;
        }
        long nextAdapterHeapBytes = heapEstimatedBytes(slot.adapter);
        adapterHeapBytes = subtractSaturating(adapterHeapBytes, slot.adapterHeapBytes);
        adapterHeapBytes = addSaturating(adapterHeapBytes, nextAdapterHeapBytes);
        slot.adapterHeapBytes = nextAdapterHeapBytes;
    }

    void close() {
        RuntimeException failure = null;
        try {
            clear();
        } catch (RuntimeException clearFailure) {
            failure = addFailure(failure, clearFailure);
        }
        if (ownsAllocator) {
            try {
                allocator.close();
            } catch (RuntimeException closeFailure) {
                failure = addFailure(failure, closeFailure);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void writeRootRecord(NativeHandle handle) {
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
            view.setLongLittleEndian(0, handle.allocatorId());
            view.setLongLittleEndian(Long.BYTES, handle.localRaw());
        }
    }

    private void validateRootRecord(NativeHandle handle) {
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
            if (view.size() != NativeStorageLayout.COLLECTION_ROOT_RECORD_BYTES) {
                throw new IllegalStateException(label + " root record size mismatch: " + handle);
            }
            NativeHandle stored = new NativeHandle(
                    view.getLongLittleEndian(0),
                    view.getLongLittleEndian(Long.BYTES)
            );
            if (!stored.equals(handle)) {
                throw new IllegalStateException(label + " root record handle mismatch: " + handle);
            }
        }
    }

    private NativeHandle requireNativeHandle(ValueHandle handle) {
        NativeHandle nativeHandle = nativeHandleOrNull(handle);
        if (nativeHandle == null) {
            throw new IllegalArgumentException("value handle is not " + label + " root: " + handle);
        }
        return nativeHandle;
    }

    private static NativeHandle nativeHandleOrNull(ValueHandle handle) {
        if (handle == null || handle.isNull()) {
            return null;
        }
        return handle.nativeHandle();
    }

    private static int mapCapacityForEntries(int entries) {
        if (entries <= 0) {
            return 0;
        }
        int capacity = 16;
        while (entries > capacity - capacity / 4) {
            if (capacity > Integer.MAX_VALUE / 2) {
                return Integer.MAX_VALUE;
            }
            capacity <<= 1;
        }
        return capacity;
    }

    private static long hashMapTableHeapBytes(int capacity) {
        return capacity == 0 ? 0L : addSaturating(ARRAY_HEADER_BYTES, (long) capacity * REFERENCE_BYTES);
    }

    private static long addSaturating(long left, long right) {
        if (left < 0 || right < 0 || Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static long subtractSaturating(long left, long right) {
        return right >= left ? 0L : left - right;
    }

    private static long nonNegative(long value) {
        return Math.max(0L, value);
    }

    private static long heapEstimatedBytes(YierdisValue adapter) {
        if (!(adapter instanceof HeapTrackedValue heapTracked)) {
            return 0L;
        }
        return nonNegative(heapTracked.heapEstimatedBytes());
    }

    private static RuntimeException addFailure(RuntimeException failure, RuntimeException next) {
        if (failure == null) {
            return next;
        }
        failure.addSuppressed(next);
        return failure;
    }

    private void failIfIterationTrapped() {
        if (iterationTrapForTesting) {
            throw new AssertionError("collection adapter iteration is forbidden while taking a memory snapshot");
        }
    }

    private static final class AdapterSlot<T extends YierdisValue> {
        private final T adapter;
        private long adapterHeapBytes;

        private AdapterSlot(T adapter, long adapterHeapBytes) {
            this.adapter = Objects.requireNonNull(adapter, "adapter");
            this.adapterHeapBytes = adapterHeapBytes;
        }
    }
}
