package yier.bubu.redis.memory.testkit;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocationGrowth;
import yier.bubu.redis.memory.api.NativeAllocationScope;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeCapacityExceededException;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.memory.api.StableMemoryBackend;

public final class FailOnAllocationStableMemoryBackend implements StableMemoryBackend {
    private final StableMemoryBackend delegate;
    private final AtomicLong attempts = new AtomicLong();
    private final Map<NativeHandle, Integer> knownCapacities = new ConcurrentHashMap<>();
    private volatile long failAt = -1L;

    public FailOnAllocationStableMemoryBackend(StableMemoryBackend delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    public void failOnAllocation(long oneBasedIndex) {
        if (oneBasedIndex <= 0L) {
            throw new IllegalArgumentException("oneBasedIndex must be > 0");
        }
        failAt = oneBasedIndex;
    }

    public void disableFailures() {
        failAt = -1L;
    }

    public void resetAttempts() {
        attempts.set(0L);
    }

    public long allocationAttempts() {
        return attempts.get();
    }

    @Override
    public NativeHandle allocate(NativeObjectKind kind, int size) {
        checkAllocationAttempt();
        NativeHandle handle = delegate.allocate(kind, size);
        rememberCapacity(handle, size);
        return handle;
    }

    @Override
    public NativeHandle reallocate(NativeHandle handle, int newSize, NativeReallocPolicy policy) {
        Objects.requireNonNull(handle, "handle");
        int currentCapacity = capacityOf(handle);
        if (newSize > currentCapacity) {
            checkAllocationAttempt();
        }
        NativeHandle resized = delegate.reallocate(handle, newSize, policy);
        knownCapacities.remove(handle);
        rememberCapacity(resized, Math.max(newSize, currentCapacity));
        return resized;
    }

    @Override
    public long allocatorId() {
        return delegate.allocatorId();
    }

    @Override
    public void bindToCurrentThread() {
        delegate.bindToCurrentThread();
    }

    @Override
    public void free(NativeHandle handle) {
        knownCapacities.remove(handle);
        delegate.free(handle);
    }

    @Override
    public void pin(NativeHandle handle) {
        delegate.pin(handle);
    }

    @Override
    public void unpin(NativeHandle handle) {
        delegate.unpin(handle);
    }

    @Override
    public NativeEpochScope beginEpoch() {
        return delegate.beginEpoch();
    }

    @Override
    public NativeAllocationScope beginAllocationScope() {
        return delegate.beginAllocationScope();
    }

    @Override
    public long estimateAllocationScopeBookkeepingBytes(int expectedAllocationCount) {
        return delegate.estimateAllocationScopeBookkeepingBytes(expectedAllocationCount);
    }

    @Override
    public NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode) {
        return delegate.resolve(handle, mode);
    }

    @Override
    public NativeObjectView resolvePinned(NativeHandle handle, NativeAccessMode mode) {
        return delegate.resolvePinned(handle, mode);
    }

    @Override
    public NativeDefragReport defragCycle(NativeDefragOptions options) {
        return delegate.defragCycle(options);
    }

    @Override
    public NativeAllocatorStats stats() {
        return delegate.stats();
    }

    @Override
    public MemoryUsageSnapshot memoryUsage() {
        return delegate.memoryUsage();
    }

    @Override
    public MemoryReclaimResult trimEmptyPages(MemoryPressureBudget budget) {
        return delegate.trimEmptyPages(budget);
    }

    @Override
    public NativeAllocationGrowth estimateAdditionalGrowth(int... requestedBytes) {
        return delegate.estimateAdditionalGrowth(requestedBytes);
    }

    @Override
    public long liveRegionCount() {
        return delegate.liveRegionCount();
    }

    @Override
    public void close() {
        knownCapacities.clear();
        delegate.close();
    }

    private void checkAllocationAttempt() {
        long attempt = attempts.incrementAndGet();
        if (attempt == failAt) {
            throw new NativeCapacityExceededException(
                    "injected stable memory allocation failure at attempt " + attempt
            );
        }
    }

    private int capacityOf(NativeHandle handle) {
        Integer known = knownCapacities.get(handle);
        if (known != null) {
            return known;
        }
        NativeObjectView view = delegate.resolve(handle, NativeAccessMode.READ_ONLY);
        if (view == null) {
            return 0;
        }
        try {
            int capacity = view.capacity();
            knownCapacities.put(handle, capacity);
            return capacity;
        } finally {
            view.close();
        }
    }

    private void rememberCapacity(NativeHandle handle, int fallback) {
        if (handle == null) {
            return;
        }
        int capacity = fallback;
        try {
            NativeObjectView view = delegate.resolve(handle, NativeAccessMode.READ_ONLY);
            if (view != null) {
                try {
                    capacity = view.capacity();
                } finally {
                    view.close();
                }
            }
        } catch (RuntimeException ignored) {
            // 测试替身可能不提供对象视图，此时用请求大小作为容量下界。
        }
        knownCapacities.put(handle, capacity);
    }
}
