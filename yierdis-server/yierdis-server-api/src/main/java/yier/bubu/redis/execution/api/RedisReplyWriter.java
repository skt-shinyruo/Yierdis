package yier.bubu.redis.execution.api;

/**
 * 把命令返回的语义回复编码到执行器已预留的 reply sink。
 * <p>
 * 该接口不是命令实现 API。命令通过 {@link RedisReply} 描述结果，执行器统一调用
 * {@link RedisReplyRenderer}，协议实现再按当前 RESP 版本编码各类标量与聚合回复。
 */
public interface RedisReplyWriter extends ReplySink {
    /**
     * 用当前槽位的控制额度替换尚未写出的预检成功回复，并输出错误。
     *
     * <p>仅用于执行期失败；已经写出业务回复后调用方必须关闭连接，不能替换客户端可见结果。</p>
     */
    default void controlError(String message) {
        error(message);
    }

    /**
     * 返回写入同一 sink、但按 {@code protocolVersion} 编码的 writer，供 {@link RedisReply.ProtocolVersioned} 使用。
     *
     * <p>不区分 RESP 版本的 writer（例如只捕获语义结构的测试 writer）可以返回自身。</p>
     */
    default RedisReplyWriter withProtocolVersion(int protocolVersion) {
        return this;
    }

    // --- Scalars ---
    void simpleString(String value);

    void error(String message);

    void integer(long value);

    // --- Aggregates ---
    void nullValue();

    void nullArray();

    void arrayHeader(int count);

    void mapHeader(int pairs);

    void setHeader(int count);
}
