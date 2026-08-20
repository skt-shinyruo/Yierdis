package yier.bubu.redis.app.server;

enum ReplySlotState {
    REGISTERED,
    WAITING_CAPACITY,
    PRODUCING,
    READY,
    WRITING,
    COMPLETING,
    CANCELLING,
    FAILING,
    COMPLETING_LEASE,
    CANCELLING_LEASE,
    FAILING_LEASE,
    COMPLETED,
    CANCELLED,
    FAILED;

    boolean cleanupOwned() {
        return switch (this) {
            case COMPLETING, CANCELLING, FAILING,
                 COMPLETING_LEASE, CANCELLING_LEASE, FAILING_LEASE,
                 COMPLETED, CANCELLED, FAILED -> true;
            default -> false;
        };
    }

    boolean cleanupInProgress() {
        return switch (this) {
            case COMPLETING, CANCELLING, FAILING,
                 COMPLETING_LEASE, CANCELLING_LEASE, FAILING_LEASE -> true;
            default -> false;
        };
    }

    boolean waitingToCloseLease() {
        return this == COMPLETING || this == CANCELLING || this == FAILING;
    }

    boolean closingLease() {
        return this == COMPLETING_LEASE || this == CANCELLING_LEASE || this == FAILING_LEASE;
    }

    ReplySlotState beginCleanup() {
        return switch (this) {
            case COMPLETED -> COMPLETING;
            case CANCELLED -> CANCELLING;
            case FAILED -> FAILING;
            default -> throw new IllegalArgumentException("not a terminal reply outcome: " + this);
        };
    }

    ReplySlotState beginLeaseClose() {
        return switch (this) {
            case COMPLETING -> COMPLETING_LEASE;
            case CANCELLING -> CANCELLING_LEASE;
            case FAILING -> FAILING_LEASE;
            default -> throw new IllegalStateException("reply lease close is not ready: " + this);
        };
    }

    ReplySlotState completeCleanup() {
        return switch (this) {
            case COMPLETING_LEASE -> COMPLETED;
            case CANCELLING_LEASE -> CANCELLED;
            case FAILING_LEASE -> FAILED;
            default -> throw new IllegalStateException("reply lease close is not in progress: " + this);
        };
    }
}
