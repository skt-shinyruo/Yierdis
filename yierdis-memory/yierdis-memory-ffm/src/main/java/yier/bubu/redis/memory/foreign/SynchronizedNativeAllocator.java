package yier.bubu.redis.memory.foreign;

import java.util.Objects;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocationGrowth;
import yier.bubu.redis.memory.api.NativeAllocationScope;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.memory.api.NativeDefragResult;
import yier.bubu.redis.memory.api.NativeEpochKind;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;

public final class SynchronizedNativeAllocator implements NativeAllocator {
    private final Object lock = new Object();
    private final NativeAllocator delegate;
    private Thread allocationScopeOwner;
    private SynchronizedAllocationScope activeAllocationScope;

    public SynchronizedNativeAllocator(YierdisFfmMemoryRuntime runtime, int maxSlots) {
        this(new YierdisStableNativeAllocator(
                runtime,
                maxSlots,
                (handle, sourceMeta, target) -> {
                },
                new YierdisAllocatorThreadGuard(false)
        ));
    }

    SynchronizedNativeAllocator(NativeAllocator delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public void bindToCurrentThread() {
        synchronized (lock) {
            delegate.bindToCurrentThread();
        }
    }

    @Override
    public NativeHandle allocate(NativeObjectKind kind, int size) {
        synchronized (lock) {
            requireAllocationScopeOwner();
            return delegate.allocate(kind, size);
        }
    }

    @Override
    public NativeHandle realloc(NativeHandle handle, int newSize, NativeReallocPolicy policy) {
        synchronized (lock) {
            requireAllocationScopeOwner();
            return delegate.realloc(handle, newSize, policy);
        }
    }

    @Override
    public void free(NativeHandle handle) {
        synchronized (lock) {
            requireAllocationScopeOwner();
            delegate.free(handle);
        }
    }

    @Override
    public void pin(NativeHandle handle) {
        synchronized (lock) {
            delegate.pin(handle);
        }
    }

    @Override
    public void unpin(NativeHandle handle) {
        synchronized (lock) {
            delegate.unpin(handle);
        }
    }

    @Override
    public NativeEpochScope beginEpoch(NativeEpochKind kind) {
        synchronized (lock) {
            return new SynchronizedEpochScope(delegate.beginEpoch(kind));
        }
    }

    @Override
    public NativeAllocationScope beginAllocationScope() {
        synchronized (lock) {
            if (allocationScopeOwner != null) {
                throw new IllegalStateException("native allocation scope is already active");
            }
            NativeAllocationScope scope = delegate.beginAllocationScope();
            allocationScopeOwner = Thread.currentThread();
            activeAllocationScope = new SynchronizedAllocationScope(scope, allocationScopeOwner);
            return activeAllocationScope;
        }
    }

    private void requireAllocationScopeOwner() {
        if (allocationScopeOwner != null && allocationScopeOwner != Thread.currentThread()) {
            throw new IllegalStateException("native allocation scope belongs to another thread");
        }
    }

    @Override
    public NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode) {
        synchronized (lock) {
            return new SynchronizedObjectView(delegate.resolve(handle, mode));
        }
    }

    @Override
    public NativeObjectView resolvePinned(NativeHandle handle, NativeAccessMode mode) {
        synchronized (lock) {
            return new SynchronizedObjectView(delegate.resolvePinned(handle, mode));
        }
    }

    @Override
    public NativeDefragResult defragOne(NativeHandle handle, long maxMoveBytes) {
        synchronized (lock) {
            return delegate.defragOne(handle, maxMoveBytes);
        }
    }

    @Override
    public NativeDefragReport defragCycle(NativeDefragOptions options) {
        synchronized (lock) {
            return delegate.defragCycle(options);
        }
    }

    @Override
    public long logicalUsedBytes() {
        synchronized (lock) {
            return delegate.logicalUsedBytes();
        }
    }

    @Override
    public NativeAllocatorStats stats() {
        synchronized (lock) {
            return delegate.stats();
        }
    }

    @Override
    public MemoryUsageSnapshot memoryUsage() {
        synchronized (lock) {
            return delegate.memoryUsage();
        }
    }

    @Override
    public MemoryReclaimResult trimEmptyPages(MemoryPressureBudget budget) {
        synchronized (lock) {
            return delegate.trimEmptyPages(budget);
        }
    }

    @Override
    public NativeAllocationGrowth estimateAdditionalGrowth(int... requestedBytes) {
        synchronized (lock) {
            return delegate.estimateAdditionalGrowth(requestedBytes);
        }
    }

    @Override
    public NativeAllocationGrowth estimateConservativeAdditionalGrowth(int... requestedBytes) {
        synchronized (lock) {
            return delegate.estimateConservativeAdditionalGrowth(requestedBytes);
        }
    }

    @Override
    public long estimateAllocationScopeBookkeepingBytes(int expectedAllocationCount) {
        synchronized (lock) {
            return delegate.estimateAllocationScopeBookkeepingBytes(expectedAllocationCount);
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            delegate.close();
        }
    }

    private final class SynchronizedEpochScope implements NativeEpochScope {
        private final NativeEpochScope delegateScope;

        private SynchronizedEpochScope(NativeEpochScope delegateScope) {
            this.delegateScope = delegateScope;
        }

        @Override
        public NativeEpochKind kind() {
            synchronized (lock) {
                return delegateScope.kind();
            }
        }

        @Override
        public long epoch() {
            synchronized (lock) {
                return delegateScope.epoch();
            }
        }

        @Override
        public void close() {
            synchronized (lock) {
                delegateScope.close();
            }
        }
    }

    private final class SynchronizedAllocationScope implements NativeAllocationScope {
        private final NativeAllocationScope delegateScope;
        private final Thread owner;
        private boolean terminal;

        private SynchronizedAllocationScope(NativeAllocationScope delegateScope, Thread owner) {
            this.delegateScope = delegateScope;
            this.owner = owner;
        }

        @Override
        public NativeAllocationGrowth growth() {
            synchronized (lock) {
                requireOwner();
                return delegateScope.growth();
            }
        }

        @Override
        public void promote() {
            synchronized (lock) {
                requireOwner();
                if (terminal) {
                    return;
                }
                delegateScope.promote();
                finish();
            }
        }

        @Override
        public void abort() {
            synchronized (lock) {
                requireOwner();
                if (terminal) {
                    return;
                }
                delegateScope.abort();
                finish();
            }
        }

        private void finish() {
            terminal = true;
            if (activeAllocationScope == this) {
                activeAllocationScope = null;
                allocationScopeOwner = null;
            }
        }

        private void requireOwner() {
            if (owner != Thread.currentThread()) {
                throw new IllegalStateException("native allocation scope belongs to another thread");
            }
        }
    }

    private final class SynchronizedObjectView implements NativeObjectView {
        private final NativeObjectView delegateView;

        private SynchronizedObjectView(NativeObjectView delegateView) {
            this.delegateView = delegateView;
        }

        @Override
        public NativeHandle handle() {
            synchronized (lock) {
                return delegateView.handle();
            }
        }

        @Override
        public int size() {
            synchronized (lock) {
                return delegateView.size();
            }
        }

        @Override
        public int capacity() {
            synchronized (lock) {
                return delegateView.capacity();
            }
        }

        @Override
        public byte getByte(int index) {
            synchronized (lock) {
                return delegateView.getByte(index);
            }
        }

        @Override
        public void setByte(int index, byte value) {
            synchronized (lock) {
                delegateView.setByte(index, value);
            }
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            synchronized (lock) {
                delegateView.getBytes(index, dst, dstOff, len);
            }
        }

        @Override
        public void setBytes(int index, byte[] src, int srcOff, int len) {
            synchronized (lock) {
                delegateView.setBytes(index, src, srcOff, len);
            }
        }

        @Override
        public void close() {
            synchronized (lock) {
                delegateView.close();
            }
        }
    }
}
