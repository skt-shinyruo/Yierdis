package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.StaleNativeHandleException;
import yier.bubu.redis.storage.memory.internal.value.HeapTrackedValue;
import yier.bubu.redis.storage.memory.internal.value.YierdisValue;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

final class NativeCollectionRootTable<T extends YierdisValue> {
    private static final int ROOT_RECORD_BYTES = Long.BYTES;
    private static final int ADAPTER_SLOTS_PER_SEGMENT = 4096;
    private static final int INITIAL_DIRECTORY_SEGMENTS = 4;
    private static final long ADAPTER_SLOT_HEAP_BYTES = 32L;

    private final NativeAllocator allocator;
    private final NativeObjectKind kind;
    private final String label;
    private final boolean ownsAllocator;
    private Object[][] adapterSegments = new Object[INITIAL_DIRECTORY_SEGMENTS][];
    private int[] adapterSegmentLiveCounts = new int[INITIAL_DIRECTORY_SEGMENTS];
    private int adapterSegmentCount;
    private long directoryHeapBytes;
    private long adapterHeapBytes;
    private boolean iterationTrapForTesting;

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
        this.directoryHeapBytes = addSaturating(
                heapBytesForObjectArray(adapterSegments.length),
                heapBytesForIntArray(adapterSegmentLiveCounts.length)
        );
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
            if (adapter instanceof HeapTrackedValue heapTracked) {
                heapTracked.setHeapChangeListener(() -> refreshAdapter(nativeHandle));
            }
            putAdapter(nativeHandle, adapter);
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
        if (nativeHandle == null) {
            return false;
        }
        try {
            validateRootRecord(nativeHandle);
            return adapterSlot(nativeHandle) != null;
        } catch (StaleNativeHandleException e) {
            return false;
        }
    }

    T require(ValueHandle handle) {
        NativeHandle nativeHandle = requireNativeHandle(handle);
        validateRootRecord(nativeHandle);
        AdapterSlot<T> slot = adapterSlot(nativeHandle);
        if (slot == null) {
            throw new IllegalArgumentException("unknown " + label + " value handle: " + handle.raw());
        }
        return slot.adapter;
    }

    long adapterBytes(ToLongFunction<? super T> estimator) {
        Objects.requireNonNull(estimator, "estimator");
        failIfIterationTrapped();
        long total = 0L;
        for (Object[] segment : adapterSegments) {
            if (segment == null) {
                continue;
            }
            for (Object value : segment) {
                if (value != null) {
                    AdapterSlot<T> slot = castSlot(value);
                    total = addSaturating(total, estimator.applyAsLong(slot.adapter));
                }
            }
        }
        return total;
    }

    long heapBytes() {
        return addSaturating(directoryHeapBytes, adapterHeapBytes);
    }

    long estimatedNewAdapterHeapGrowthBytes(long adapterValueHeapBytes) {
        return estimatedNewAdapterHeapGrowthBytes(adapterValueHeapBytes, 1);
    }

    long estimatedNewAdapterHeapGrowthBytes(long adapterValueHeapBytes, int expectedNativeAllocationCount) {
        if (expectedNativeAllocationCount < 1) {
            throw new IllegalArgumentException("expectedNativeAllocationCount must be >= 1");
        }
        long growth = addSaturating(ADAPTER_SLOT_HEAP_BYTES, nonNegative(adapterValueHeapBytes));
        int possibleSegmentCount = possibleAdapterSegmentCount(expectedNativeAllocationCount);
        int highestPossibleSegmentIndex = possibleSegmentCount - 1;
        growth = addSaturating(growth, directoryExpansionAllocationBytes(highestPossibleSegmentIndex));
        for (int segmentIndex = 0; segmentIndex <= highestPossibleSegmentIndex; segmentIndex++) {
            if (segmentIndex >= adapterSegments.length || adapterSegments[segmentIndex] == null) {
                growth = addSaturating(growth, heapBytesForObjectArray(ADAPTER_SLOTS_PER_SEGMENT));
            }
        }
        return growth;
    }

    void release(ValueHandle handle) {
        if (handle == null) {
            return;
        }
        NativeHandle nativeHandle = requireNativeHandle(handle);
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
        } catch (RuntimeException e) {
            failure = addFailure(failure, e);
        }
        try {
            allocator.free(nativeHandle);
            rootFreed = true;
        } catch (StaleNativeHandleException ignored) {
            // 存活性由 allocator 判定；外部已 free 的 root 只说明 adapter 表里残留了可收敛的 stale 项。
            rootFreed = true;
        } catch (RuntimeException e) {
            failure = addFailure(failure, e);
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
        for (Object[] segment : adapterSegments) {
            if (segment == null) {
                continue;
            }
            for (Object value : segment) {
                if (value == null) {
                    continue;
                }
                AdapterSlot<T> slot = castSlot(value);
                try {
                    release(ValueHandle.fromRaw(slot.rawHandle));
                } catch (RuntimeException e) {
                    failure = addFailure(failure, e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    int adapterSegmentCount() {
        return adapterSegmentCount;
    }

    void refreshAdapter(ValueHandle handle) {
        if (handle == null || handle.isNull()) {
            return;
        }
        refreshAdapter(handle.nativeHandle());
    }

    void armIterationTrapForTesting() {
        iterationTrapForTesting = true;
    }

    void disarmIterationTrapForTesting() {
        iterationTrapForTesting = false;
    }

    private void putAdapter(NativeHandle handle, T adapter) {
        int segmentIndex = adapterSegmentIndex(handle);
        ensureDirectoryCapacity(segmentIndex);
        Object[] segment = adapterSegments[segmentIndex];
        if (segment == null) {
            segment = new Object[ADAPTER_SLOTS_PER_SEGMENT];
            adapterSegments[segmentIndex] = segment;
            adapterSegmentCount++;
            directoryHeapBytes = addSaturating(directoryHeapBytes, heapBytesForObjectArray(segment.length));
        }
        int offset = adapterSegmentOffset(handle);
        if (segment[offset] != null) {
            throw new IllegalStateException(label + " adapter slot is already occupied: " + handle.slotId());
        }
        long adapterValueHeapBytes = heapEstimatedBytes(adapter);
        segment[offset] = new AdapterSlot<>(handle.raw(), adapter, adapterValueHeapBytes);
        adapterSegmentLiveCounts[segmentIndex]++;
        adapterHeapBytes = addSaturating(adapterHeapBytes, ADAPTER_SLOT_HEAP_BYTES);
        adapterHeapBytes = addSaturating(adapterHeapBytes, adapterValueHeapBytes);
    }

    private AdapterSlot<T> adapterSlot(NativeHandle handle) {
        int segmentIndex = adapterSegmentIndex(handle);
        if (segmentIndex >= adapterSegments.length) {
            return null;
        }
        Object[] segment = adapterSegments[segmentIndex];
        if (segment == null) {
            return null;
        }
        Object value = segment[adapterSegmentOffset(handle)];
        if (value == null) {
            return null;
        }
        AdapterSlot<T> slot = castSlot(value);
        return slot.rawHandle == handle.raw() ? slot : null;
    }

    private void removeAdapter(NativeHandle handle, AdapterSlot<T> expected) {
        int segmentIndex = adapterSegmentIndex(handle);
        if (segmentIndex >= adapterSegments.length) {
            return;
        }
        Object[] segment = adapterSegments[segmentIndex];
        if (segment == null) {
            return;
        }
        int offset = adapterSegmentOffset(handle);
        if (segment[offset] != expected) {
            return;
        }
        segment[offset] = null;
        adapterHeapBytes = subtractSaturating(adapterHeapBytes, ADAPTER_SLOT_HEAP_BYTES);
        adapterHeapBytes = subtractSaturating(adapterHeapBytes, expected.adapterHeapBytes);
        int remaining = --adapterSegmentLiveCounts[segmentIndex];
        if (remaining == 0) {
            adapterSegments[segmentIndex] = null;
            adapterSegmentCount--;
            directoryHeapBytes = subtractSaturating(directoryHeapBytes, heapBytesForObjectArray(segment.length));
        }
    }

    private void ensureDirectoryCapacity(int segmentIndex) {
        if (segmentIndex < adapterSegments.length) {
            return;
        }
        int nextLength = adapterSegments.length;
        while (nextLength <= segmentIndex) {
            nextLength = Math.multiplyExact(nextLength, 2);
        }
        long previousDirectoryArrays = addSaturating(
                heapBytesForObjectArray(adapterSegments.length),
                heapBytesForIntArray(adapterSegmentLiveCounts.length)
        );
        adapterSegments = Arrays.copyOf(adapterSegments, nextLength);
        adapterSegmentLiveCounts = Arrays.copyOf(adapterSegmentLiveCounts, nextLength);
        long nextDirectoryArrays = addSaturating(
                heapBytesForObjectArray(adapterSegments.length),
                heapBytesForIntArray(adapterSegmentLiveCounts.length)
        );
        directoryHeapBytes = subtractSaturating(directoryHeapBytes, previousDirectoryArrays);
        directoryHeapBytes = addSaturating(directoryHeapBytes, nextDirectoryArrays);
    }

    private int possibleAdapterSegmentCount(int expectedNativeAllocationCount) {
        NativeAllocatorStats stats = allocator.stats();
        long activeSegments = Math.max(0L, stats.activeMetadataSegments());
        long freeSlots = Math.max(0L, stats.freeSlots());
        long additionalAllocations = Math.max(0L, (long) expectedNativeAllocationCount - freeSlots);
        long additionalSegments = divideRoundingUp(additionalAllocations, ADAPTER_SLOTS_PER_SEGMENT);
        long totalSegments = addSaturating(activeSegments, additionalSegments);
        if (totalSegments == 0L) {
            totalSegments = 1L;
        }
        return totalSegments > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) totalSegments;
    }

    private long directoryExpansionAllocationBytes(int segmentIndex) {
        if (segmentIndex < adapterSegments.length) {
            return 0L;
        }
        int nextLength = adapterSegments.length;
        while (nextLength <= segmentIndex) {
            if (nextLength > Integer.MAX_VALUE / 2) {
                return Long.MAX_VALUE;
            }
            nextLength *= 2;
        }
        return addSaturating(
                heapBytesForObjectArray(nextLength),
                heapBytesForIntArray(nextLength)
        );
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

    private static int adapterSegmentIndex(NativeHandle handle) {
        return Math.toIntExact((handle.slotId() - 1L) / ADAPTER_SLOTS_PER_SEGMENT);
    }

    private static int adapterSegmentOffset(NativeHandle handle) {
        return Math.toIntExact((handle.slotId() - 1L) % ADAPTER_SLOTS_PER_SEGMENT);
    }

    @SuppressWarnings("unchecked")
    private static <T extends YierdisValue> AdapterSlot<T> castSlot(Object value) {
        return (AdapterSlot<T>) value;
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

    private static long subtractSaturating(long left, long right) {
        return right >= left ? 0L : left - right;
    }

    private static long divideRoundingUp(long value, long divisor) {
        if (value == 0L) {
            return 0L;
        }
        return (value - 1L) / divisor + 1L;
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

    private static long heapBytesForObjectArray(int length) {
        return addSaturating(16L, (long) length * Long.BYTES);
    }

    private static long heapBytesForIntArray(int length) {
        return addSaturating(16L, (long) length * Integer.BYTES);
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
        private final long rawHandle;
        private final T adapter;
        private long adapterHeapBytes;

        private AdapterSlot(long rawHandle, T adapter, long adapterHeapBytes) {
            this.rawHandle = rawHandle;
            this.adapter = Objects.requireNonNull(adapter, "adapter");
            this.adapterHeapBytes = adapterHeapBytes;
        }
    }
}
