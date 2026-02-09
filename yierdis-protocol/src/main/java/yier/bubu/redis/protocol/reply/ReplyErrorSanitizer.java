package yier.bubu.redis.protocol.reply;

import java.util.Objects;

/**
 * 协议错误 message 的统一净化/限长（SSOT）。
 * <p>
 * 目标：防止 CRLF 注入（response splitting），并稳定错误输出长度，避免实现漂移。
 */
public final class ReplyErrorSanitizer {
    public static final int DEFAULT_MAX_ERROR_MESSAGE_CHARS = 256;

    private ReplyErrorSanitizer() {
    }

    public static String sanitize(ReplyErrorKind kind, String message) {
        return sanitize(kind, message, DEFAULT_MAX_ERROR_MESSAGE_CHARS);
    }

    public static String sanitize(ReplyErrorKind kind, String message, int maxChars) {
        Objects.requireNonNull(kind, "kind");
        int limit = Math.max(0, maxChars);

        String msg = message;
        if (msg == null || msg.isBlank()) {
            msg = defaultMessage(kind);
        }
        msg = msg.replace('\r', ' ').replace('\n', ' ');
        if (limit > 0 && msg.length() > limit) {
            msg = msg.substring(0, limit);
        }
        return msg;
    }

    private static String defaultMessage(ReplyErrorKind kind) {
        return switch (kind) {
            case PROTOCOL -> "Protocol error";
            case COMMAND -> "ERR error";
            case INTERNAL -> "ERR internal error";
        };
    }
}

