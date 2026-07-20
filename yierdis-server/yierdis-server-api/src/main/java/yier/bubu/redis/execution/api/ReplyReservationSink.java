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

    long writtenBytes();

    /**
     * 接受已写入来源的所有权；返回 false 表示调用方仍负责关闭资源。
     */
    default boolean transferOwnership(AutoCloseable resource) {
        return false;
    }
}
