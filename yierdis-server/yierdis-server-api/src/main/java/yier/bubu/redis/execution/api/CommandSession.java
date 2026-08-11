package yier.bubu.redis.execution.api;

/**
 * 命令执行所需的完整连接会话。
 *
 * <p>DB 路由只需要选择状态，因此保留独立的 {@link DbIndexSession} 窄接口；其余能力总是随命令会话
 * 一起消费，不再为单一实现暴露额外接口。</p>
 */
public interface CommandSession extends DbIndexSession {
    long clientId();

    String clientName();

    void setClientName(String clientName);

    boolean authenticated();

    void setAuthenticated(boolean authenticated);

    TransactionState transaction();

    ConnectionStatsView connectionStats();

    int respVersion();

    void setRespVersion(int respVersion);
}
