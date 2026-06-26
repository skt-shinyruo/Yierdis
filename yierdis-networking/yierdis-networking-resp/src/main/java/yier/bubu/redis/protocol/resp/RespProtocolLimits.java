package yier.bubu.redis.protocol.resp;

public final class RespProtocolLimits {
    public static final int DEFAULT_MAX_BULK_BYTES = 512 * 1024 * 1024;
    public static final int DEFAULT_MAX_ARGS = 1024 * 1024;
    public static final int DEFAULT_MAX_INLINE_BYTES = 1024 * 1024;
    public static final int DEFAULT_MAX_COMMAND_BYTES = 64 * 1024 * 1024;
    public static final int MAX_BULK_BYTES = DEFAULT_MAX_BULK_BYTES;
    public static final int MAX_ARGS = DEFAULT_MAX_ARGS;
    public static final int MAX_COMMAND_BYTES = DEFAULT_MAX_COMMAND_BYTES;

    private RespProtocolLimits() {
    }
}
