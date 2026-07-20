package yier.bubu.redis.memory.api;

import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;

public interface NativeAllocator extends AutoCloseable {
    default void bindToCurrentThread() {
    }

    NativeHandle allocate(NativeObjectKind kind, int size);

    /**
     * 为 primitive 数据结构分配对象并直接返回编码句柄。
     */
    default long allocateRaw(NativeObjectKind kind, int size) {
        return allocate(kind, size).raw();
    }

    NativeHandle realloc(NativeHandle handle, int newSize, NativeReallocPolicy policy);

    /**
     * 调整 primitive 容器中的稳定句柄对象；返回值保持同一 raw identity。
     */
    default long reallocRaw(long rawHandle, int newSize, NativeReallocPolicy policy) {
        return realloc(NativeHandle.fromRaw(rawHandle), newSize, policy).raw();
    }

    void free(NativeHandle handle);

    /**
     * 释放 primitive 容器中的句柄；实现可覆盖此方法以避免在热路径物化 {@link NativeHandle}。
     */
    default void freeRaw(long rawHandle) {
        free(NativeHandle.fromRaw(rawHandle));
    }

    void pin(NativeHandle handle);

    /**
     * 固定 primitive 容器中的句柄；生命周期与 {@link #pin(NativeHandle)} 相同。
     */
    default void pinRaw(long rawHandle) {
        pin(NativeHandle.fromRaw(rawHandle));
    }

    void unpin(NativeHandle handle);

    /**
     * 解除 primitive 句柄的固定；调用次数必须与 {@link #pinRaw(long)} 匹配。
     */
    default void unpinRaw(long rawHandle) {
        unpin(NativeHandle.fromRaw(rawHandle));
    }

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
     * 解析 primitive 容器中的句柄；返回视图的关闭与固定语义不变。
     */
    default NativeObjectView resolveRaw(long rawHandle, NativeAccessMode mode) {
        return resolve(NativeHandle.fromRaw(rawHandle), mode);
    }

    /**
     * Resolves a read-only view while the caller retains a separate pin on the handle.
     */
    default NativeObjectView resolvePinned(NativeHandle handle, NativeAccessMode mode) {
        return resolve(handle, mode);
    }

    /**
     * 解析已由调用方固定的 primitive 句柄，不转移该外部 pin 的所有权。
     */
    default NativeObjectView resolvePinnedRaw(long rawHandle, NativeAccessMode mode) {
        return resolvePinned(NativeHandle.fromRaw(rawHandle), mode);
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
