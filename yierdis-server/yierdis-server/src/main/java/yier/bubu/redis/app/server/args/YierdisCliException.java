package yier.bubu.redis.app.server.args;

/**
 * 表示可预期的用户配置或参数错误，使启动入口能返回稳定退出码而不打印未知异常堆栈。
 */
public final class YierdisCliException extends IllegalArgumentException {
    private final int exitCode;

    private YierdisCliException(String message, int exitCode, Throwable cause) {
        super(message, cause);
        this.exitCode = exitCode;
    }

    public int exitCode() {
        return exitCode;
    }

    public static YierdisCliException usageError(String message, Throwable cause) {
        return new YierdisCliException(message, 2, cause);
    }
}
