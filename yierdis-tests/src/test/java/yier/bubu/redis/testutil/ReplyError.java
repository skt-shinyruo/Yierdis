package yier.bubu.redis.testutil;

import java.util.Objects;

public record ReplyError(String message) implements ReplyObject {
    public ReplyError {
        Objects.requireNonNull(message, "message");
    }
}
