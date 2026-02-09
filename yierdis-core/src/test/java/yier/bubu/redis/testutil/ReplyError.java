package yier.bubu.redis.testutil;

import java.util.Objects;

public final class ReplyError implements ReplyObject {
    public enum Kind {
        COMMAND,
        PROTOCOL,
        INTERNAL
    }

    private final Kind kind;
    private final String message;

    public ReplyError(Kind kind, String message) {
        this.kind = kind == null ? Kind.COMMAND : kind;
        this.message = Objects.requireNonNull(message, "message");
    }

    public Kind kind() {
        return kind;
    }

    public String message() {
        return message;
    }
}

