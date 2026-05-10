package yier.bubu.redis.protocol.resp;

public enum RespProtocolVersion {
    RESP2(2),
    RESP3(3);

    private final int wireValue;

    RespProtocolVersion(int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static RespProtocolVersion fromWireValue(int value) {
        return switch (value) {
            case 2 -> RESP2;
            case 3 -> RESP3;
            default -> throw new IllegalArgumentException("NOPROTO unsupported protocol version");
        };
    }
}
