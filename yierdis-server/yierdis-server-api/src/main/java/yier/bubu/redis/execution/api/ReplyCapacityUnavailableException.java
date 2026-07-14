package yier.bubu.redis.execution.api;

/**
 * 当前全局或连接回复额度不足，但稍后可能恢复。
 */
public final class ReplyCapacityUnavailableException extends RuntimeException {
    public ReplyCapacityUnavailableException(String message) {
        super(message);
    }
}
