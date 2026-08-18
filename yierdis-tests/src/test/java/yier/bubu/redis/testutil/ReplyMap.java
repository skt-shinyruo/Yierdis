package yier.bubu.redis.testutil;

import java.util.List;
import java.util.Objects;

public record ReplyMap(List<Entry> entries) implements ReplyObject {
    public record Entry(ReplyObject key, ReplyObject value) {
        public Entry {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
        }
    }

    public ReplyMap(List<Entry> entries) {
        this.entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
    }
}
