package yier.bubu.redis.app.server.args;

/**
 * 表示可预期的用户配置或参数错误。
 */
public final class YierdisCliException extends IllegalArgumentException {
    private YierdisCliException(String message, Throwable cause) {
        super(message, cause);
    }

    public static YierdisCliException usageError(String message, Throwable cause) {
        return new YierdisCliException(message, cause);
    }
}
