package yier.bubu.redis.storage.memory.internal.ledger;

// MemoryLedgerOutOfMemoryException：ledger 层的“预算触顶”拒写异常（对齐 Redis OOM message）。

/**
 * Thrown when a reservation would exceed {@code maxmemory} budget.
 * <p>
 * Message contract: aligned with Redis OOM error string to keep client expectations stable.
 */
public final class MemoryLedgerOutOfMemoryException extends RuntimeException {
    public static final String REDIS_OOM_MESSAGE = "OOM command not allowed when used memory > 'maxmemory'.";

    public MemoryLedgerOutOfMemoryException() {
        super(REDIS_OOM_MESSAGE);
    }
}
