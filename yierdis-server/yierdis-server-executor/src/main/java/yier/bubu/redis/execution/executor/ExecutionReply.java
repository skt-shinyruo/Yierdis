package yier.bubu.redis.execution.executor;

import yier.bubu.redis.bytes.BytesSink;

/**
 * 由传输层创建、由 executor 在一次命令执行期间持有的回复所有权。
 */
public interface ExecutionReply extends AutoCloseable {
    BytesSink sink();

    /**
     * 在回复预检暂时没有额度时注册一次唤醒；返回 false 表示该回复已不能继续等待。
     */
    default boolean awaitCapacity(Runnable wakeup) {
        return false;
    }

    void markReady(boolean closeAfterReply);

    void cancel();

    boolean hasWrittenBytes();

    default void markResultUnknown() {
    }

    @Override
    void close();
}
