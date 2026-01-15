package yier.bubu.redis.protocol;

public final class RespError implements RespObject {
    private final String message;

    private RespError(String message) {
        this.message = message;
    }

    public static RespError of(String message) {
        return new RespError(message);
    }

    public String message() {
        return message;
    }

    @Override
    public RespType type() {
        return RespType.ERROR;
    }

    @Override
    public String toHumanReadableString() {
        return message;
    }
}
