package yier.bubu.redis.protocol;

// RESP3 big number（(number）对象：以字符串形式保留，避免大整数溢出与额外依赖。

import java.util.Objects;

public final class RespBigNumber implements RespObject {
    private final String value;

    private RespBigNumber(String value) {
        this.value = Objects.requireNonNull(value, "value");
    }

    public static RespBigNumber of(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("value must not be blank");
        }
        return new RespBigNumber(value);
    }

    public String value() {
        return value;
    }

    @Override
    public RespType type() {
        return RespType.BIG_NUMBER;
    }

    @Override
    public String toHumanReadableString() {
        return value;
    }
}

