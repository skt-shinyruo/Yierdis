package yier.bubu.redis.protocol.reply;

/**
 * IR null 值（语义为 JSON null / Redis null bulk string / null array 等）。
 */
public enum ReplyNull implements ReplyValue {
    INSTANCE
}

