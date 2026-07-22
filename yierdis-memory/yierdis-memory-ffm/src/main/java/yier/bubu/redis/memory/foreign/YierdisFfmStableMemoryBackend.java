package yier.bubu.redis.memory.foreign;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.MemoryOwner;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocationGrowth;
import yier.bubu.redis.memory.api.NativeAllocationScope;
import yier.bubu.redis.memory.api.NativeAllocatorMetadataStats;
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
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.StableMemoryRegion;

public final class YierdisFfmStableMemoryBackend implements StableMemoryBackend {
    private final long allocatorId;
    private final MemoryOwner owner;
    private final YierdisFfmMemoryRuntime runtime;
    private final YierdisStableNativeAllocator allocator;
    private final AtomicLong externalRegionBytes = new AtomicLong();

    public YierdisFfmStableMemoryBackend(String name, int maxSlots, MemoryOwner owner) {
        this.allocatorId = yier.bubu.redis.memory.api.StableMemoryBackendIds.nextId();
        this.owner = Objects.requireNonNull(owner, "owner");
        this.runtime = new YierdisFfmMemoryRuntime(Objects.requireNonNull(name, "name"));
        this.allocator = new YierdisStableNativeAllocator(runtime, maxSlots, allocatorId, owner);
    }

    @Override
    public long allocatorId() {
        return allocatorId;
    }

    @Override
    public void bindToCurrentThread() {
        allocator.bindToCurrentThread();
    }

    @Override
    public NativeHandle allocate(NativeObjectKind kind, int size) {
        return allocator.allocate(kind, size);
    }

    @Override
    public NativeHandle reallocate(NativeHandle handle, int newSize, NativeReallocPolicy policy) {
        return allocator.reallocate(handle, newSize, policy);
    }

    @Override
    public void free(NativeHandle handle) {
        allocator.free(handle);
    }

    @Override
    public void pin(NativeHandle handle) {
        allocator.pin(handle);
    }

    @Override
    public void unpin(NativeHandle handle) {
        allocator.unpin(handle);
    }

    @Override
    public NativeEpochScope beginEpoch(NativeEpochKind kind) {
        return allocator.beginEpoch(kind);
    }

    @Override
    public NativeAllocationScope beginAllocationScope() {
        return allocator.beginAllocationScope();
    }

    @Override
    public long estimateAllocationScopeBookkeepingBytes(int expectedAllocationCount) {
        return allocator.estimateAllocationScopeBookkeepingBytes(expectedAllocationCount);
    }

    @Override
    public NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode) {
        return allocator.resolve(handle, mode);
    }

    @Override
    public NativeObjectView resolvePinned(NativeHandle handle, NativeAccessMode mode) {
        return allocator.resolvePinned(handle, mode);
    }

    @Override
    public StableMemoryRegion allocateRegion(String regionOwner, int bytes) {
        owner.checkCurrentThread();
        Objects.requireNonNull(regionOwner, "regionOwner");
        if (bytes <= 0) {
            throw new IllegalArgumentException("bytes must be > 0");
        }
        YierdisFfmRegion region = runtime.allocateRegion(regionOwner, bytes);
        externalRegionBytes.addAndGet(bytes);
        return new TrackingRegion(region, bytes);
    }

    @Override
    public NativeDefragResult defragOne(NativeHandle handle, long maxMoveBytes) {
        return allocator.defragOne(handle, maxMoveBytes);
    }

    @Override
    public NativeDefragReport defragCycle(NativeDefragOptions options) {
        return allocator.defragCycle(options);
    }

    @Override
    public long logicalUsedBytes() {
        return allocator.logicalUsedBytes();
    }

    @Override
    public NativeAllocatorStats stats() {
        return allocator.stats();
    }

    @Override
    public NativeAllocatorMetadataStats metadataStats() {
        return allocator.metadataStats();
    }

    private MemoryUsageSnapshot externalRegionUsage() {
        long bytes = externalRegionBytes.get();
        return new MemoryUsageSnapshot(0L, 0L, bytes, bytes, 0L);
    }

    @Override
    public MemoryUsageSnapshot memoryUsage() {
        return allocator.memoryUsage().plus(externalRegionUsage());
    }

    @Override
    public MemoryReclaimResult trimEmptyPages(MemoryPressureBudget budget) {
        return allocator.trimEmptyPages(budget);
    }

    @Override
    public NativeAllocationGrowth estimateAdditionalGrowth(int... requestedBytes) {
        return allocator.estimateAdditionalGrowth(requestedBytes);
    }

    @Override
    public NativeAllocationGrowth estimateConservativeAdditionalGrowth(int... requestedBytes) {
        return allocator.estimateConservativeAdditionalGrowth(requestedBytes);
    }

    @Override
    public long liveRegionCount() {
        owner.checkCurrentThread();
        return runtime.liveRegionCount();
    }

    @Override
    public void close() {
        owner.checkCurrentThreadForShutdown();
        RuntimeException failure = null;
        try {
            allocator.close();
        } catch (RuntimeException closeFailure) {
            failure = closeFailure;
        }
        try {
            runtime.close();
        } catch (RuntimeException closeFailure) {
            if (failure == null) {
                failure = closeFailure;
            } else {
                failure.addSuppressed(closeFailure);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private final class TrackingRegion implements StableMemoryRegion {
        private final YierdisFfmRegion delegate;
        private final int bytes;
        private final AtomicBoolean closed = new AtomicBoolean();

        private TrackingRegion(YierdisFfmRegion delegate, int bytes) {
            this.delegate = delegate;
            this.bytes = bytes;
        }

        @Override
        public int size() {
            checkOpen();
            return delegate.size();
        }

        @Override
        public byte getByte(int offset) {
            checkOpen();
            return delegate.getByte(offset);
        }

        @Override
        public void setByte(int offset, byte value) {
            checkOpen();
            delegate.setByte(offset, value);
        }

        @Override
        public int getIntLittleEndian(int offset) {
            checkOpen();
            return delegate.getIntLittleEndian(offset);
        }

        @Override
        public void setIntLittleEndian(int offset, int value) {
            checkOpen();
            delegate.setIntLittleEndian(offset, value);
        }

        @Override
        public long getLongLittleEndian(int offset) {
            checkOpen();
            return delegate.getLongLittleEndian(offset);
        }

        @Override
        public void setLongLittleEndian(int offset, long value) {
            checkOpen();
            delegate.setLongLittleEndian(offset, value);
        }

        @Override
        public void getBytes(int offset, byte[] dst, int dstOffset, int length) {
            checkOpen();
            delegate.getBytes(offset, dst, dstOffset, length);
        }

        @Override
        public void setBytes(int offset, byte[] src, int srcOffset, int length) {
            checkOpen();
            delegate.setBytes(offset, src, srcOffset, length);
        }

        @Override
        public void copyTo(int sourceOffset, StableMemoryRegion target, int targetOffset, int length) {
            checkOpen();
            Objects.requireNonNull(target, "target");
            if (target instanceof TrackingRegion trackingTarget) {
                trackingTarget.checkOpen();
                delegate.copyTo(sourceOffset, trackingTarget.delegate, targetOffset, length);
                return;
            }
            delegate.copyTo(sourceOffset, target, targetOffset, length);
        }

        @Override
        public void close() {
            owner.checkCurrentThread();
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            delegate.close();
            long remaining = externalRegionBytes.addAndGet(-bytes);
            if (remaining < 0L) {
                throw new IllegalStateException("external region accounting underflow");
            }
        }

        private void checkOpen() {
            owner.checkCurrentThread();
            if (closed.get()) {
                throw new IllegalStateException("stable memory region is closed");
            }
        }
    }
}
