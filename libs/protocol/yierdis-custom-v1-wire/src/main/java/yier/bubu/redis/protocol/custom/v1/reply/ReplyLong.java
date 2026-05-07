package yier.bubu.redis.protocol.custom.v1.reply;

/**
 * 协议侧 integer 值（long）。
 */
public record ReplyLong(long value) implements ReplyValue {
}
