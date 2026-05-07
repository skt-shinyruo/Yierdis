package yier.bubu.redis.protocol.custom.v1.reply;

/**
 * 协议侧 boolean 值。
 */
public record ReplyBoolean(boolean value) implements ReplyValue {
}
