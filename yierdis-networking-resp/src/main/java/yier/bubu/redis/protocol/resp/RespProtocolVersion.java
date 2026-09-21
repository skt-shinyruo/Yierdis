package yier.bubu.redis.protocol.resp;

import java.nio.charset.StandardCharsets;

/**
 * RESP 协议版本，同时是 RESP2/RESP3 编码差异的唯一定义：sizer 与 writer 都从这里取
 * null 编码字节与聚合头的版本决策。任何未来类型（double、bignum、verbatim 等）的版本差异
 * 也必须只在这里展开一次，两侧才能保持逐字节一致。
 */
public enum RespProtocolVersion {
    RESP2(2, "$-1\r\n", "*-1\r\n", '*', 2, '*'),
    RESP3(3, "_\r\n", "_\r\n", '%', 1, '~');

    private final int wireValue;
    // null 编码字节常量构造后不再修改：writer 原样写出，sizer 按同一组常量的长度记账，两侧不会逐字节漂移。
    private final byte[] nullValueEncoding;
    private final byte[] nullArrayEncoding;
    private final char mapPrefix;
    private final int mapCountFactor;
    private final char setPrefix;

    RespProtocolVersion(
            int wireValue,
            String nullValueEncoding,
            String nullArrayEncoding,
            char mapPrefix,
            int mapCountFactor,
            char setPrefix
    ) {
        this.wireValue = wireValue;
        this.nullValueEncoding = nullValueEncoding.getBytes(StandardCharsets.US_ASCII);
        this.nullArrayEncoding = nullArrayEncoding.getBytes(StandardCharsets.US_ASCII);
        this.mapPrefix = mapPrefix;
        this.mapCountFactor = mapCountFactor;
        this.setPrefix = setPrefix;
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

    byte[] nullValueEncoding() {
        return nullValueEncoding;
    }

    byte[] nullArrayEncoding() {
        return nullArrayEncoding;
    }

    char mapPrefix() {
        return mapPrefix;
    }

    // RESP2 没有 map 类型，map 头退化为展平的 array 头，计数是键值对数的两倍。
    long mapHeaderCount(long pairs) {
        return pairs * mapCountFactor;
    }

    char setPrefix() {
        return setPrefix;
    }
}
