package yier.bubu.redis.execution.api;

import yier.bubu.redis.bytes.BytesSink;

/**
 * 在第一次写入前接受回复预检额度的字节 sink。
 */
public interface ReplyReservationSink extends BytesSink {
    void require(ReplyPlan plan) throws ReplyCapacityUnavailableException, ReplyTooLargeException;

    long writtenBytes();

    /**
     * 接受已写入来源的所有权；返回 false 表示调用方仍负责关闭资源。
     */
    default boolean transferOwnership(AutoCloseable resource) {
        return false;
    }
}
