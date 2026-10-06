package yier.bubu.redis.client;

/**
 * 连接没建立，或读写、超限 bulk、回复中的 {@code %}、{@code ~}、{@code _} 已经把这条连接关掉。不自动重试。
 */
public class ConnectionException extends RuntimeException {
    public ConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
