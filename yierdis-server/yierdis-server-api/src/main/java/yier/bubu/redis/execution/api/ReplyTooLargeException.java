package yier.bubu.redis.execution.api;

/**
 * 单个回复即使在空闲时也无法满足配置上限。
 */
public final class ReplyTooLargeException extends RuntimeException {
    public ReplyTooLargeException(String message) {
        super(message);
    }
}
