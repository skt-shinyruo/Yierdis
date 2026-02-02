package yier.bubu.redis.protocol;

// RESP3 boolean（#t/#f）对象：用于 CLI/测试解析与类型断言。

public final class RespBoolean implements RespObject {
    private static final RespBoolean TRUE = new RespBoolean(true);
    private static final RespBoolean FALSE = new RespBoolean(false);

    private final boolean value;

    private RespBoolean(boolean value) {
        this.value = value;
    }

    public static RespBoolean of(boolean value) {
        return value ? TRUE : FALSE;
    }

    public boolean value() {
        return value;
    }

    @Override
    public RespType type() {
        return RespType.BOOLEAN;
    }

    @Override
    public String toHumanReadableString() {
        return value ? "true" : "false";
    }
}

