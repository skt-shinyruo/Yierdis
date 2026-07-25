package yier.bubu.redis.execution.api;

/**
 * Redis command reply model used by the command layer.
 * <p>
 * This interface is not a generic protocol writer. It exposes the Redis reply shapes that command
 * handlers produce, including RESP2-compatible scalars and the RESP3/Redis aggregate forms used by
 * HELLO, INFO/STATS, and collection replies. Protocol implementations encode these shapes into the
 * active wire format without making command handlers depend on protocol packages.
 */
public interface RedisReplyWriter extends ReplySink {
    void requestCloseAfterReply();

    boolean closeAfterReplyRequested();

    /**
     * 用当前槽位的控制额度替换尚未写出的预检成功回复，并输出错误。
     *
     * <p>仅用于执行期失败；已经写出业务回复后调用方必须关闭连接，不能替换客户端可见结果。</p>
     */
    default void controlError(String message) {
        error(message);
    }

    /**
     * Marks the current reply as a protocol-level error.
     * <p>
     * This is distinct from command-layer {@link #error(String)} values, which are part of the command semantics and
     * may appear inside aggregates (e.g. EXEC's result array).
     */
    default void protocolError(String message) {
        error(message);
    }

    /**
     * Marks the current reply as an internal/server error.
     * <p>
     * Protocol implementations may encode this differently from command errors ({@link #error(String)}).
     */
    default void internalError(String message) {
        error(message);
    }

    // --- Scalars ---
    void simpleString(String value);

    void error(String message);

    void integer(long value);

    void booleanValue(boolean value);

    void doubleValue(double value);

    void bigNumberAscii(String value);

    void verbatimString(String format, byte[] data);

    void blobError(String message);

    // --- Aggregates ---
    void nullValue();

    void nullArray();

    void arrayHeader(int count);

    void emptyArray();

    void mapHeader(int pairs);

    void setHeader(int count);

    void pushHeader(int count);

    void attributeHeader(int pairs);
}
