package yier.bubu.redis.protocol;

public final class RespSimpleString implements RespObject {
    private final String value;

    private RespSimpleString(String value) {
        this.value = value;
    }

    public static RespSimpleString of(String value) {
        return new RespSimpleString(value);
    }

    public String value() {
        return value;
    }

    @Override
    public RespType type() {
        return RespType.SIMPLE_STRING;
    }

    @Override
    public String toHumanReadableString() {
        return value;
    }
}
