package yier.bubu.redis.memory.api;

/**
 * 为稳定内存后端提供显式 owner thread 绑定与校验。
 * 普通访问不会隐式绑定；未绑定实例可以执行关闭校验，绑定后只能由 owner thread 关闭。
 */
public interface MemoryOwner {
    /** 将未绑定 owner 绑定到当前线程；同一线程重复绑定有效，跨线程绑定失败。 */
    void bindToCurrentThread();

    /** 校验普通访问来自已绑定的 owner thread；绑定前或跨线程访问失败。 */
    void checkCurrentThread();

    /** 校验关闭来自 owner thread；尚未绑定时允许关闭，绑定后拒绝跨线程关闭。 */
    void checkCurrentThreadForShutdown();
}
