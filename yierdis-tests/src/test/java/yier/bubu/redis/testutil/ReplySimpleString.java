package yier.bubu.redis.testutil;

import java.util.Objects;

public record ReplySimpleString(String value) implements ReplyObject {
    public ReplySimpleString {
        Objects.requireNonNull(value, "value");
    }
}
