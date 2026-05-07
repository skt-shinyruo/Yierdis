package yier.bubu.redis.protocol.custom.v1.wire;

/**
 * Custom Protocol v1 default safety limits (SSOT).
 * <p>
 * These limits protect against user-controlled input causing DoS/memory retention issues and are shared
 * defaults for server/client/decoders.
 */
public final class ProtocolLimits {
    /**
     * Default max request payload bytes (length-prefixed JSON payload).
     */
    public static final int DEFAULT_MAX_REQUEST_PAYLOAD_BYTES = 64 * 1024 * 1024; // 64 MiB

    /**
     * Default max args per command (argv-style).
     */
    public static final int DEFAULT_MAX_ARGS = 1024;

    /**
     * Default max header bytes for protocol framing.
     * <p>
     * For Custom Protocol v1, this bounds the length field before ':' (and acts as a general "line/header" safety cap).
     */
    public static final int DEFAULT_MAX_HEADER_BYTES = 64 * 1024;

    private ProtocolLimits() {
    }
}
