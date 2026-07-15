package yier.bubu.redis.memory.api;

import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;

public interface NativeAllocator extends AutoCloseable {
    default void bindToCurrentThread() {
    }

    NativeHandle allocate(NativeObjectKind kind, int size);

    NativeHandle realloc(NativeHandle handle, int newSize, NativeReallocPolicy policy);

    void free(NativeHandle handle);

    void pin(NativeHandle handle);

    void unpin(NativeHandle handle);

    NativeEpochScope beginEpoch(NativeEpochKind kind);

    default NativeAllocationScope beginAllocationScope() {
        throw new UnsupportedOperationException("native allocation scopes are not supported");
    }

    default long estimateAllocationScopeBookkeepingBytes(int expectedAllocationCount) {
        if (expectedAllocationCount < 0) {
            throw new IllegalArgumentException("expectedAllocationCount must be >= 0");
        }
        return 0L;
    }

    NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode);

    /**
     * Resolves a read-only view while the caller retains a separate pin on the handle.
     */
    default NativeObjectView resolvePinned(NativeHandle handle, NativeAccessMode mode) {
        return resolve(handle, mode);
    }

    NativeDefragResult defragOne(NativeHandle handle, long maxMoveBytes);

    NativeDefragReport defragCycle(NativeDefragOptions options);

    default long logicalUsedBytes() {
        return Math.max(0L, stats().logicalUsedBytes());
    }

    NativeAllocatorStats stats();

    default NativeAllocatorMetadataStats metadataStats() {
        NativeAllocatorStats stats = stats();
        return new NativeAllocatorMetadataStats(stats.activeMetadataSegments(), stats.freeSlots());
    }

    default MemoryUsageSnapshot memoryUsage() {
        NativeAllocatorStats stats = stats();
        return new MemoryUsageSnapshot(0, 0, stats.committedBytes(), stats.reservedBytes(), stats.freeBytes());
    }

    default MemoryReclaimResult trimEmptyPages(MemoryPressureBudget budget) {
        return MemoryReclaimResult.empty();
    }

    default NativeAllocationGrowth estimateAdditionalGrowth(int... requestedBytes) {
        throw new UnsupportedOperationException("native allocation growth estimation is not supported");
    }

    default NativeAllocationGrowth estimateConservativeAdditionalGrowth(int... requestedBytes) {
        return estimateAdditionalGrowth(requestedBytes);
    }

    @Override
    void close();
}
