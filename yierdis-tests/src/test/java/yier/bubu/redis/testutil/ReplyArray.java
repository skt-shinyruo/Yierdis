package yier.bubu.redis.testutil;

import java.util.List;
import java.util.Objects;

public record ReplyArray(List<ReplyObject> values) implements ReplyObject {
    public ReplyArray(List<ReplyObject> values) {
        this.values = List.copyOf(Objects.requireNonNull(values, "values"));
    }
}
