package yier.bubu.redis.protocol.reply;

/**
 * 协议侧 null 值（语义为 JSON null / Redis null bulk string / null array 等）。
 */
public enum ReplyNull implements ReplyValue {
    INSTANCE
}
