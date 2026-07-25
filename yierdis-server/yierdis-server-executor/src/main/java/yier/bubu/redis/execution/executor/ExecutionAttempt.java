package yier.bubu.redis.execution.executor;

public enum ExecutionAttempt {
    COMPLETED,
    REPREPARE,
    REPLY_CAPACITY_BLOCKED,
    CONNECTION_CLOSED
}
