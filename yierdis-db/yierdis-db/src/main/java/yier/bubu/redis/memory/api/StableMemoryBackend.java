package yier.bubu.redis.memory.api;

import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;

/**
 * 提供稳定对象、回收和统计能力的 owner-bound 后端。
 * 每个实例具有进程内不复用的正 {@code allocatorId}；句柄必须先校验该身份，
 * 再由所属后端解释 {@code localRaw}。内存访问须显式绑定，所有视图、scope、epoch
 * 和显式 pin 都须在后端关闭前结束。
 */
public interface StableMemoryBackend extends AutoCloseable {
    long allocatorId();

    /** 显式绑定 owner；任何普通内存访问都不得代替调用方完成绑定。 */
    void bindToCurrentThread();

    NativeHandle allocate(NativeObjectKind kind, int size);

    /** 调整容量并保持完整句柄身份不变。 */
    NativeHandle reallocate(NativeHandle handle, int newSize, NativeReallocPolicy policy);

    void free(NativeHandle handle);

    /** 保留对象；每次成功调用都必须由同一 owner 配对调用 {@link #unpin(NativeHandle)}。 */
    void pin(NativeHandle handle);

    void unpin(NativeHandle handle);

    NativeEpochScope beginEpoch();

    NativeAllocationScope beginAllocationScope();

    long estimateAllocationScopeBookkeepingBytes(int expectedAllocationCount);

    /** 返回拥有自身保留的视图；关闭视图会释放该保留。 */
    NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode);

    /**
     * 借用调用方已经持有的 pin，只接受 {@link NativeAccessMode#READ_ONLY}；
     * 关闭视图不会替调用方执行 {@link #unpin(NativeHandle)}。
     */
    NativeObjectView resolvePinned(NativeHandle handle, NativeAccessMode mode);

    NativeDefragReport defragCycle(NativeDefragOptions options);

    NativeAllocatorStats stats();

    MemoryUsageSnapshot memoryUsage();

    MemoryReclaimResult trimEmptyPages(MemoryPressureBudget budget);

    NativeAllocationGrowth estimateAdditionalGrowth(int... requestedBytes);

    long liveRegionCount();

    /** 按 owner 的 shutdown 规则释放后端；调用前必须先结束所有派生资源和显式 pin。 */
    @Override
    void close();
}
