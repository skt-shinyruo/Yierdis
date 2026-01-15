package yier.bubu.redis.protocol;

/**
 * Connection/session scoped state for RESP processing.
 * <p>
 * The primary responsibility is tracking the negotiated RESP protocol version (RESP2 vs RESP3).
 * Netty-specific session implementations live in adapter modules.
 */
public interface RespSession {
    RespProtocol protocol();

    void setProtocol(RespProtocol protocol);
}
