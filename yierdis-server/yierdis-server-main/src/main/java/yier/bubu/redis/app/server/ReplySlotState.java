package yier.bubu.redis.app.server;

enum ReplySlotState {
    REGISTERED,
    WAITING_CAPACITY,
    PRODUCING,
    READY,
    WRITING,
    COMPLETED,
    CANCELLED,
    FAILED
}
