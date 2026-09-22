package yier.bubu.redis.app.server;

/**
 * 回复槽的终止结果，进入清理时确定，不随清理 phase 变化。
 */
enum ReplySlotOutcome {
    COMPLETED,
    CANCELLED,
    FAILED
}
