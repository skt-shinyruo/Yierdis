package yier.bubu.redis.protocol.reply;

import java.util.Objects;

/**
 * IR error 值（可作为顶层 envelope 或嵌套值）。
 */
public record ReplyError(ReplyErrorKind kind, String message) implements ReplyValue {
    public ReplyError {
        Objects.requireNonNull(kind, "kind");
    }
}

