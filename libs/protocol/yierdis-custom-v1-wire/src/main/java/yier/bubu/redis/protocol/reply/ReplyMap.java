package yier.bubu.redis.protocol.reply;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 协议侧 map 值（entry 列表，key/value 均为 ReplyValue）。
 * <p>
 * 注意：该模型不依赖 JSON object key 只能为 string 的限制。
 */
public final class ReplyMap implements ReplyValue {
    public record Entry(ReplyValue key, ReplyValue value) {
        public Entry {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
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
