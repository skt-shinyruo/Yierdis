package yier.bubu.redis.protocol;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * RESP3 set（最小子集）。
 * <p>
 * 注意：RESP3 set 允许元素为任意 {@link RespObject}，这里用 list 保持顺序与兼容性。
 */
public final class RespSet implements RespObject {
    private final List<RespObject> values;

    private RespSet(List<RespObject> values) {
        this.values = values;
    }

    public static RespSet of(List<RespObject> values) {
        Objects.requireNonNull(values, "values");
        return new RespSet(Collections.unmodifiableList(new ArrayList<>(values)));
    }

    public List<RespObject> values() {
        return values;
    }

    @Override
    public RespType type() {
        return RespType.SET;
    }

    @Override
    public String toHumanReadableString() {
        return values.toString();
    }
}

