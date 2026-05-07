package yier.bubu.redis.protocol.custom.v1.reply;

/**
 * 协议侧 string 值（UTF-16 Java String）。
 */
public record ReplyString(String value) implements ReplyValue {
}
