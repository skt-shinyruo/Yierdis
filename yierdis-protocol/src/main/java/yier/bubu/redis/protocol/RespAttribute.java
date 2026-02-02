package yier.bubu.redis.protocol;

// RESP3 attribute（|pairs\r\n... + 后续 reply）对象：在一个 frame 中同时保留 attributes 与真正的 reply。

import java.util.Objects;

public final class RespAttribute implements RespObject {
    private final RespMap attributes;
    private final RespObject value;

    private RespAttribute(RespMap attributes, RespObject value) {
        this.attributes = Objects.requireNonNull(attributes, "attributes");
        this.value = Objects.requireNonNull(value, "value");
    }

    public static RespAttribute of(RespMap attributes, RespObject value) {
        return new RespAttribute(attributes, value);
    }

    public RespMap attributes() {
        return attributes;
    }

    public RespObject value() {
        return value;
    }

    @Override
    public RespType type() {
        return RespType.ATTRIBUTE;
    }

    @Override
    public String toHumanReadableString() {
        return "attributes=" + attributes.toHumanReadableString() + ", value=" + value.toHumanReadableString();
    }
}

