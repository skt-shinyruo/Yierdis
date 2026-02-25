package yier.bubu.redis.protocol.reply;

/**
 * IR string 值（UTF-16 Java String）。
 */
public record ReplyString(String value) implements ReplyValue {
}
