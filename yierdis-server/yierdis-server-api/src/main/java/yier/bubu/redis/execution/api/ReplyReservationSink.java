package yier.bubu.redis.execution.api;

import yier.bubu.redis.bytes.BytesSink;

/**
 * 在第一次写入前接受回复预检额度的字节 sink。
 */
public interface ReplyReservationSink extends BytesSink {
    void require(ReplyPlan plan) throws ReplyCapacityUnavailableException, ReplyTooLargeException;

    /**
     * 将尚未写出的业务回复切换为槽位已有的控制回复额度。
     *
     * <p>调用方只能在本回复尚未写出任何字节时调用；不具备控制额度的实现可保留默认行为。</p>
     */
    default void useControlReservation() {
    }

    long writtenBytes();
}
