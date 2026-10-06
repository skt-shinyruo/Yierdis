package yier.bubu.redis.client;

/**
 * 参数在写出前编不成 UTF-8，或已经读完的回复里有非法 UTF-8。连接保持可用。
 */
public class DecodeException extends RuntimeException {
    public DecodeException(String message, Throwable cause) {
        super(message, cause);
    }
}
