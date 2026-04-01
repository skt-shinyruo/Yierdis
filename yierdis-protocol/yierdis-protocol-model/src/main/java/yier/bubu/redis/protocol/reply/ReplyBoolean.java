package yier.bubu.redis.protocol.reply;

/**
 * 协议侧 boolean 值。
 */
public record ReplyBoolean(boolean value) implements ReplyValue {
}
