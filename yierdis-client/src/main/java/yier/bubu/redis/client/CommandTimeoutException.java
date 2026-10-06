package yier.bubu.redis.client;

/**
 * 命令读超时。连接已关闭。这不是 {@link ConnectionException} 的子类。
 */
public class CommandTimeoutException extends RuntimeException {
    public CommandTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
