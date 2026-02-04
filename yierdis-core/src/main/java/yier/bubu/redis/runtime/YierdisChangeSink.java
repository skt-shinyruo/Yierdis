package yier.bubu.redis.runtime;

// YierdisChangeSink：变更事件的消费入口（可用于 AOF/replication/审计等），默认 NOOP。

import java.util.Objects;

@FunctionalInterface
public interface YierdisChangeSink {
    YierdisChangeSink NOOP = event -> {
    };

    void onChange(YierdisChangeEvent event);

    static YierdisChangeSink noop() {
        return NOOP;
    }

    static YierdisChangeSink requireNonNull(YierdisChangeSink sink) {
        return Objects.requireNonNull(sink, "sink");
    }
}

