package yier.bubu.redis.execution.api;

/**
 * 把命令返回的语义回复编码到执行器已预留的 reply sink。
 * <p>
 * 该接口不是命令实现 API。命令通过 {@link RedisReply} 描述结果，执行器统一调用
 * {@link RedisReplyRenderer}，协议实现再按当前 RESP 版本编码各类标量与聚合回复。
 */
public interface RedisReplyWriter extends ReplySink {
    /**
     * 请求在当前控制回复写完后关闭连接；普通命令的关闭语义由 {@link CommandResult} 携带。
     */
    void requestCloseAfterReply();

    /**
     * 返回协议或执行器控制路径是否已请求在当前回复后关闭连接。
     */
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

    void mapHeader(int pairs);

    void setHeader(int count);

    void pushHeader(int count);

    void attributeHeader(int pairs);
}
