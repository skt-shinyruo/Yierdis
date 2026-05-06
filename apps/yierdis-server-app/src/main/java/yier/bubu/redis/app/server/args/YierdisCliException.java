package yier.bubu.redis.app.server.args;

/**
 * 用于表示“可预期的用户配置/参数错误”的异常类型。
 * <p>
 * 该异常旨在让调用方区分“用户可修复的问题”和“程序 bug/未知异常”，并提供稳定的退出码语义。
 */
public final class YierdisCliException extends IllegalArgumentException {
    private final int exitCode;
    private final boolean shouldPrintUsage;

    public YierdisCliException(String message, int exitCode, boolean shouldPrintUsage, Throwable cause) {
        super(message, cause);
        this.exitCode = exitCode;
        this.shouldPrintUsage = shouldPrintUsage;
    }

    public YierdisCliException(String message, int exitCode, boolean shouldPrintUsage) {
        this(message, exitCode, shouldPrintUsage, null);
    }

    public int exitCode() {
        return exitCode;
    }

    public boolean shouldPrintUsage() {
        return shouldPrintUsage;
    }

    public static YierdisCliException usageError(String message, Throwable cause) {
        return new YierdisCliException(message, 2, true, cause);
    }

    public static YierdisCliException userError(String message, Throwable cause) {
        return new YierdisCliException(message, 2, false, cause);
    }
}

