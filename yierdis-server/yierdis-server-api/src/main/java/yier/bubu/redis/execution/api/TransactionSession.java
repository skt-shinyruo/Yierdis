package yier.bubu.redis.execution.api;

/**
 * Connection-scoped MULTI/EXEC/DISCARD state.
 */
public interface TransactionSession {
    TransactionState transaction();
}
