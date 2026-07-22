package yier.bubu.redis.memory.api;

/**
 * 跟踪同一后端在单一活动分配范围内新建的对象。
 * {@link #promote()} 提交对象并把后续释放责任交给调用方；{@link #abort()} 释放仍被跟踪的对象；
 * {@link #close()} 等同 abort，因此成功路径必须显式 promote。终止操作可重复调用且不影响后续 scope。
 */
public interface NativeAllocationScope extends AutoCloseable {
    NativeAllocationGrowth growth();

    void promote();

    void abort();

    @Override
    default void close() {
        abort();
    }
}
