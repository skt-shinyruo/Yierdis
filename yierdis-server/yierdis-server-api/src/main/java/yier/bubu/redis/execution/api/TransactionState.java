package yier.bubu.redis.execution.api;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Connection-scoped transaction state (MULTI/EXEC/DISCARD) abstraction.
 * <p>
 * This lives in the contract module so that the command layer can access connection state without depending on
 * transport/server implementations.
 */
public interface TransactionState {
    boolean active();

    void begin();

    void discard();

    void enqueue(ExecutionRequest request);

    default boolean aborted() {
        return false;
    }

    default void markAborted() {
        // no-op
    }

    default String tryEnqueue(ExecutionRequest request) {
        if (request == null) {
            return null;
        }
        enqueue(request);
        return null;
    }

    int size();

    /**
     * 在不转移队列所有权的前提下计算完整 {@code EXEC} 回复上界。
     *
     * <p>不支持只读遍历的实现必须回退 maximum，不能通过 {@link #drain()} 临时取得请求。</p>
     */
    default ReplyPlan planExecReply(Function<? super ExecutionRequest, ReplyPlan> planner) {
        Objects.requireNonNull(planner, "planner");
        return ReplyPlan.maximum();
    }

    List<ExecutionRequest> drain();
}
