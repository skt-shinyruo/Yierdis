package yier.bubu.redis.storage.api;

/**
 * 一次显式 reconcileAccounting 对账的结果视图：成功时携带对账前后的账本数字，
 * 失败时携带失败类型与消息。
 * <p>
 * driftBytes 带符号：正值表示物理用量超出 ledger 估算（补记入账），负值表示估算高估。
 * 对账失败（物理重算不可用）时 physicalUsedBytes 为 -1，driftBytes 为 0——未产出可信物理值，
 * 账本不做任何修正，degraded 状态保持不变。
 */
public record DbAccountingReconciliation(
        boolean succeeded,
        long ledgerUsedBeforeBytes,
        long physicalUsedBytes,
        long driftBytes,
        String failureTypeName,
        String failureMessage,
        long attemptedAtMillis
) {
    public DbAccountingReconciliation {
        if (ledgerUsedBeforeBytes < 0L) {
            throw new IllegalArgumentException("ledgerUsedBeforeBytes must be >= 0");
        }
        if (attemptedAtMillis <= 0L) {
            throw new IllegalArgumentException("attemptedAtMillis must be > 0");
        }
        if (succeeded) {
            if (physicalUsedBytes < 0L) {
                throw new IllegalArgumentException("physicalUsedBytes must be >= 0 on success");
            }
            if (failureTypeName != null || failureMessage != null) {
                throw new IllegalArgumentException("successful reconciliation must not carry failure details");
            }
        } else if (failureTypeName == null) {
            throw new IllegalArgumentException("failed reconciliation requires failureTypeName");
        }
    }

    public static DbAccountingReconciliation success(
            long ledgerUsedBeforeBytes,
            long physicalUsedBytes,
            long attemptedAtMillis
    ) {
        return new DbAccountingReconciliation(
                true,
                ledgerUsedBeforeBytes,
                physicalUsedBytes,
                physicalUsedBytes - ledgerUsedBeforeBytes,
                null,
                null,
                attemptedAtMillis
        );
    }

    public static DbAccountingReconciliation failure(
            long ledgerUsedBeforeBytes,
            Throwable failure,
            long attemptedAtMillis
    ) {
        if (failure == null) {
            throw new IllegalArgumentException("failure must not be null");
        }
        return new DbAccountingReconciliation(
                false,
                ledgerUsedBeforeBytes,
                -1L,
                0L,
                failure.getClass().getName(),
                failure.getMessage(),
                attemptedAtMillis
        );
    }
}
