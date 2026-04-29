package yier.bubu.redis.testutil;

import java.util.Objects;

public final class ReplySimpleString implements ReplyObject {
    private final String value;

    public ReplySimpleString(String value) {
        this.value = Objects.requireNonNull(value, "value");
    }

    public String value() {
        return value;
    }
}

