package yier.bubu.redis.protocol;

// RESP3 double（,number）对象：用于 CLI/测试解析与类型断言。

public final class RespDouble implements RespObject {
    private final double value;

    private RespDouble(double value) {
        this.value = value;
    }

    public static RespDouble of(double value) {
        return new RespDouble(value);
    }

    public double value() {
        return value;
    }

    @Override
    public RespType type() {
        return RespType.DOUBLE;
    }

    @Override
    public String toHumanReadableString() {
        return Double.toString(value);
    }
}

