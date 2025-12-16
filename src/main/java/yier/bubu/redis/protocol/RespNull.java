package yier.bubu.redis.protocol;

/**
 * Internal placeholder for RESP null (used when parsing *-1).
 * For Redis clients, null is typically represented as $-1 or *-1.
 */
public final class RespNull implements RespObject {
    public static final RespNull INSTANCE = new RespNull();

    private RespNull() {
    }

    @Override
    public RespType type() {
        return RespType.NULL;
    }

    @Override
    public String toHumanReadableString() {
        return "(null)";
    }
}
