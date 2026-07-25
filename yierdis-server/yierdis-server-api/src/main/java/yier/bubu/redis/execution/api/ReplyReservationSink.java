package yier.bubu.redis.execution.api;

import yier.bubu.redis.bytes.BytesSink;

import java.util.Objects;

/**
 * 在第一次写入前接受回复预检额度的字节 sink。
 */
public interface ReplyReservationSink extends BytesSink {
    void require(ReplyPlan plan) throws ReplyCapacityUnavailableException, ReplyTooLargeException;

    /**
     * 预留在后续嵌套 {@link #require(ReplyPlan)} 调用期间仍保持有效的顶层 envelope。
     *
     * <p>未实现该能力的 sink 必须回退 maximum；精确实现需要同时约束总编码字节和保留来源。</p>
     */
    default void requireEnvelope(ReplyPlan plan)
            throws ReplyCapacityUnavailableException, ReplyTooLargeException {
        Objects.requireNonNull(plan, "plan");
        require(ReplyPlan.maximum());
    }

    /**
     * 将尚未写出的业务回复切换为槽位已有的控制回复额度。
     *
     * <p>调用方只能在本回复尚未写出任何字节时调用；不具备控制额度的实现可保留默认行为。</p>
     */
    default void useControlReservation() {
    }

    long writtenBytes();

    /**
     * 接受已写入来源的所有权；返回 false 表示调用方仍负责关闭资源。
     */
    default boolean transferOwnership(AutoCloseable resource) {
        return false;
    }
}
