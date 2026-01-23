package yier.bubu.redis.protocol;

/**
 * RESP 协议相关的默认安全上限（SSOT）。
 * <p>
 * 这些上限用于防止用户可控输入导致的 DoS/内存驻留问题，并作为 server/client/parser 的共同默认值来源。
 */
public final class RespLimits {
    /**
     * 单个 bulk string 的默认最大长度（bytes）。
     */
    public static final int DEFAULT_MAX_BULK_BYTES = 64 * 1024 * 1024; // 64 MiB

    /**
     * 单条命令的默认最大参数个数。
     */
    public static final int DEFAULT_MAX_ARGS = 1024;

    /**
     * 单行（以 CRLF 结尾）的默认最大长度（bytes）。
     * <p>
     * 说明：该值同时影响 inline command 与 RESP header line（如 $len / *argc）。
     * 需要在“调试可用性”与“DoS 防护”之间平衡，因此默认提升到 64KiB。
     */
    public static final int DEFAULT_MAX_LINE_BYTES = 64 * 1024;

    /**
     * 数组/Map 的默认最大元素数（用于 reply 解码与对象解析）。
     */
    public static final int DEFAULT_MAX_ARRAY_LEN = 1024;

    /**
     * 默认最大嵌套深度（用于 reply 解码与对象解析）。
     */
    public static final int DEFAULT_MAX_NESTING_DEPTH = 64;

    private RespLimits() {
    }
}

