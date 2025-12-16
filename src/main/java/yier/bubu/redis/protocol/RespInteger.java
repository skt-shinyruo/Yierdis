package yier.bubu.redis.protocol;

public final class RespInteger implements RespObject {
    private final long value;

    private RespInteger(long value) {
        this.value = value;
    }

    public static RespInteger of(long value) {
        return new RespInteger(value);
    }

    public long value() {
        return value;
    }

    @Override
    public RespType type() {
        return RespType.INTEGER;
    }

    @Override
    public String toHumanReadableString() {
        return Long.toString(value);
    }
}
