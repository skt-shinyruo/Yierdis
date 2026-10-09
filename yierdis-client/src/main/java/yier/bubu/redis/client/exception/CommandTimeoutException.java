package yier.bubu.redis.client.exception;

/**
 * 命令超时。从写出开始，直到调用方拿到的 Future 完成；事务命令算到 {@code EXEC} 的结果，不是 {@code QUEUED}。
 * 只失败这一条，连接保持打开，迟到的回复仍按写出顺序吸收。这不是 {@link ConnectionException} 的子类。
 */
public class CommandTimeoutException extends RuntimeException {
    public CommandTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
