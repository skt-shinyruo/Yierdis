package yier.bubu.redis.client.exception;

/**
 * 在借出等待内没有拿到连接，或池已经关闭。不关闭已经借出的连接。
 * 这不是 {@link CommandTimeoutException}，也不是 {@link ConnectionException}。
 */
public class BorrowException extends RuntimeException {
    public BorrowException(String message) {
        super(message);
    }

    public BorrowException(String message, Throwable cause) {
        super(message, cause);
    }
}
