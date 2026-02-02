package yier.bubu.redis.protocol;

// Server 侧可扩展的连接态接口：用于在不引入 Netty 依赖的前提下，让命令层访问必要的会话状态（例如 dbIndex、认证与 client name）。

public interface RespServerSession extends RespSession {
    /**
     * 当前连接选择的 DB index（Redis 风格：连接级状态）。
     */
    int dbIndex();

    /**
     * 设置当前连接选择的 DB index。
     */
    void setDbIndex(int dbIndex);

    /**
     * 连接级 client id（用于 CLIENT ID / 诊断）。
     */
    long clientId();

    /**
     * 连接级 client name（用于 CLIENT SETNAME / GETNAME / HELLO SETNAME）。
     */
    String clientName();

    /**
     * 设置连接级 client name。
     */
    void setClientName(String clientName);

    /**
     * 是否已通过 AUTH（在 requirepass 模式下生效）。
     */
    boolean authenticated();

    /**
     * 设置认证状态。
     */
    void setAuthenticated(boolean authenticated);

    /**
     * 连接级事务状态（MULTI/EXEC/DISCARD）。
     */
    RespTransactionState transaction();
}
