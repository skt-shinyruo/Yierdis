package yier.bubu.redis.protocol;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * RESP3 map（最小子集）。
 * <p>
 * 注意：为保持解码顺序与兼容性，这里使用 list-of-pairs 结构，而不是强制转换成 {@link java.util.Map}。
 */
public final class RespMap implements RespObject {
    private final List<Entry> entries;

    private RespMap(List<Entry> entries) {
        this.entries = entries;
    }

    public static RespMap of(List<Entry> entries) {
        Objects.requireNonNull(entries, "entries");
        return new RespMap(Collections.unmodifiableList(new ArrayList<>(entries)));
    }

    public List<Entry> entries() {
        return entries;
    }

    @Override
    public RespType type() {
        return RespType.MAP;
    }

    @Override
    public String toHumanReadableString() {
        return entries.toString();
    }

    public static final class Entry {
        private final RespObject key;
        private final RespObject value;

        public Entry(RespObject key, RespObject value) {
            this.key = key;
            this.value = value;
        }

        public RespObject key() {
            return key;
        }

        public RespObject value() {
            return value;
        }
    }
}

