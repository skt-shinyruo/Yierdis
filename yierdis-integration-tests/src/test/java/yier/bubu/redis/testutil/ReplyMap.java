package yier.bubu.redis.testutil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class ReplyMap implements ReplyObject {
    public static final class Entry {
        private final ReplyObject key;
        private final ReplyObject value;

        public Entry(ReplyObject key, ReplyObject value) {
            this.key = Objects.requireNonNull(key, "key");
            this.value = Objects.requireNonNull(value, "value");
        }

        public ReplyObject key() {
            return key;
        }

        public ReplyObject value() {
            return value;
        }
    }

    private final List<Entry> entries;

    public ReplyMap(List<Entry> entries) {
        Objects.requireNonNull(entries, "entries");
        this.entries = Collections.unmodifiableList(new ArrayList<>(entries));
    }

    public List<Entry> entries() {
        return entries;
    }
}

