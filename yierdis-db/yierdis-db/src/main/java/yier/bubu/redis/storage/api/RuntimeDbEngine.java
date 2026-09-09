package yier.bubu.redis.storage.api;

public interface RuntimeDbEngine extends DbEngine {
    void bindToCurrentThread();
    void runMaintenance();

    default void runDeferredReclamation() {
    }

    default void defragMaintenance() {
    }

    /**
     * 显式对账内存记账：重算物理用量、把 ledger 漂移修正入账，成功时清除 invariant-failure
     * degraded 状态并恢复写入；每次尝试与结果写入 {@link #health()} 快照供外部观测。
     * <p>
     * 只能在 owner thread 上调用（与 maintenance 同一 thread guard）。恢复不会自动发生——
     * 持续性记账 bug 必须仍以事故形式暴露，由运维显式触发恢复。
     */
    default DbAccountingReconciliation reconcileAccounting() {
        throw new UnsupportedOperationException("reconcileAccounting is not supported");
    }

    void shutdown();
}
