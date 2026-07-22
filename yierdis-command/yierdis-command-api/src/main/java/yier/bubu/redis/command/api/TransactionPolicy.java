package yier.bubu.redis.command.api;

public enum TransactionPolicy {
    QUEUEABLE,
    TRANSACTION_CONTROL,
    DISALLOWED_IN_MULTI
}
