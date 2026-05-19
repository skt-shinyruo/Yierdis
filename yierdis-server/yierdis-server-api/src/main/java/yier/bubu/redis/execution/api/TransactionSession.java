package yier.bubu.redis.execution.api;

/**
 * Connection-scoped MULTI/EXEC/DISCARD state.
 */
public interface TransactionSession extends Session {
    TransactionState transaction();
}
