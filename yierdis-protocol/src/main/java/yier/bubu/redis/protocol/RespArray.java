package yier.bubu.redis.protocol;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class RespArray implements RespObject {
    private static final RespArray NULL = new RespArray(null);

    private final List<RespObject> values;

    private RespArray(List<RespObject> values) {
        this.values = values;
    }

    public static RespArray of(List<RespObject> values) {
        return new RespArray(Collections.unmodifiableList(new ArrayList<>(values)));
    }

    public static RespArray empty() {
        return of(Collections.<RespObject>emptyList());
    }

    public static RespArray nullArray() {
        return NULL;
    }

    public boolean isNull() {
        return values == null;
    }

    public List<RespObject> values() {
        return values == null ? null : values;
    }

    @Override
    public RespType type() {
        return RespType.ARRAY;
    }

    @Override
    public String toHumanReadableString() {
        return values == null ? "(null)" : values.toString();
    }
}
