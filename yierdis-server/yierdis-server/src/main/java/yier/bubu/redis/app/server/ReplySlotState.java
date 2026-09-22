package yier.bubu.redis.app.server;

/**
 * 回复槽的线性生命周期。终止结果由 {@link ReplySlotOutcome} 记录，
 * 清理中的租约关闭 phase 是槽内的布尔标志，不再派生笛卡尔状态。
 */
enum ReplySlotState {
    REGISTERED,
    WAITING_CAPACITY,
    PRODUCING,
    READY,
    WRITING,
    CLEANING,
    TERMINATED;

    boolean cleanupOwned() {
        return switch (this) {
            case CLEANING, TERMINATED -> true;
            default -> false;
        };
    }

    boolean cleanupInProgress() {
        return switch (this) {
            case CLEANING -> true;
            default -> false;
        };
    }
}
