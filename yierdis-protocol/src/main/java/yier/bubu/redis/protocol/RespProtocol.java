package yier.bubu.redis.protocol;

/**
 * Connection-level RESP protocol version.
 * <p>
 * Default is RESP2; a client may switch to RESP3 using {@code HELLO 3}.
 */
public enum RespProtocol {
    RESP2(2),
    RESP3(3);

    private final int proto;

    RespProtocol(int proto) {
        this.proto = proto;
    }

    public int proto() {
        return proto;
    }
}
