package yier.bubu.redis.protocol;

// RESP3 push（>count\r\n...）对象：用于 PubSub 等异步消息的解析与断言。

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class RespPush implements RespObject {
    private final List<RespObject> values;

    private RespPush(List<RespObject> values) {
        this.values = values;
    }

    public static RespPush of(List<RespObject> values) {
        if (values == null) {
            throw new IllegalArgumentException("values must not be null");
        }
        return new RespPush(Collections.unmodifiableList(new ArrayList<>(values)));
    }

    public List<RespObject> values() {
        return values;
    }

    @Override
    public RespType type() {
        return RespType.PUSH;
    }

    @Override
    public String toHumanReadableString() {
        return values.toString();
    }
}

