package yier.bubu.redis.protocol.reply;

/**
 * IR integer 值（long）。
 */
public record ReplyLong(long value) implements ReplyValue {
}
