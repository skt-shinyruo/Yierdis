package yier.bubu.redis.client;

/**
 * 参数在写出前编不成 UTF-8，已经读完的回复里有非法 UTF-8，或类型化方法读到的回复形状对不上。
 * 当前回复已经读完，连接可以继续用。
 */
public class DecodeException extends RuntimeException {
    public DecodeException(String message, Throwable cause) {
        super(message, cause);
    }
}
