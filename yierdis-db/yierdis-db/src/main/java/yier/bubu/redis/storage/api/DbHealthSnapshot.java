package yier.bubu.redis.storage.api;

public record DbHealthSnapshot(
        boolean degraded,
        String failureTypeName,
        String failureMessage,
        long failureAtMillis,
        DbAccountingReconciliation lastReconciliation
) {
    private static final DbHealthSnapshot HEALTHY = new DbHealthSnapshot(false, null, null, 0L, null);

    public static DbHealthSnapshot healthy() {
        return HEALTHY;
    }

    public static DbHealthSnapshot degraded(Throwable failure, long failureAtMillis) {
        if (failure == null) {
            throw new IllegalArgumentException("failure must not be null");
        }
        if (failureAtMillis <= 0L) {
            throw new IllegalArgumentException("failureAtMillis must be > 0");
        }
        return new DbHealthSnapshot(
                true,
                failure.getClass().getName(),
                failure.getMessage(),
                failureAtMillis,
                null
        );
    }

    /**
     * 组合恢复流程产生的健康视图：失败字段描述当前未恢复的 degraded episode，对账成功后 episode
     * 关闭、失败字段随之清除，对账结果留在 lastReconciliation 供审计。
     * lastReconciliation 为 null 表示从未尝试对账。
     */
    public static DbHealthSnapshot of(
            boolean degraded,
            String failureTypeName,
            String failureMessage,
            long failureAtMillis,
            DbAccountingReconciliation lastReconciliation
    ) {
        if (degraded && failureTypeName == null) {
            throw new IllegalArgumentException("degraded snapshot requires failureTypeName");
        }
        if (degraded && failureAtMillis <= 0L) {
            throw new IllegalArgumentException("degraded snapshot requires failureAtMillis > 0");
        }
        return new DbHealthSnapshot(
                degraded,
                failureTypeName,
                failureMessage,
                failureAtMillis,
                lastReconciliation
        );
    }
}
