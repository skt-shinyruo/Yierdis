package yier.bubu.redis.protocol.resp;

public final class RespProtocolLimits {
    public static final int DEFAULT_MAX_BULK_BYTES = 512 * 1024 * 1024;
    public static final int DEFAULT_MAX_ARGS = 1024 * 1024;
    public static final int DEFAULT_MAX_INLINE_BYTES = 1024 * 1024;

    private RespProtocolLimits() {
    }
}
