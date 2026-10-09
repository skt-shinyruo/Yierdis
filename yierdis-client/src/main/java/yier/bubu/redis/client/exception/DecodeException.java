package yier.bubu.redis.client.exception;

/**
 * 参数在写出前编不成 UTF-8，这时还没有回复，命令不会发出。
 * 已经读完的回复里有非法 UTF-8，或类型化方法读到的回复形状对不上。这两种连接可以继续用。
 */
public class DecodeException extends RuntimeException {
    public DecodeException(String message, Throwable cause) {
        super(message, cause);
    }
}
