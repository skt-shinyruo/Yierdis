package yier.bubu.redis.app.server;

enum ReplyCleanupOwner {
    NONE,
    SEQUENCER,
    FINAL_WRITE_FUTURE,
    CONNECTION_CLOSE,
    SHUTDOWN
}
