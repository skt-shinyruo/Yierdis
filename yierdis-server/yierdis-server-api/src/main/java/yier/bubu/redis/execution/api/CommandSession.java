package yier.bubu.redis.execution.api;

/**
 * 命令执行所需的完整连接会话。
 *
 * <p>DB 路由和命令执行都直接消费同一个会话。</p>
 */
public interface CommandSession {
    int dbIndex();

    void setDbIndex(int dbIndex);

    String clientName();

    void setClientName(String clientName);

    TransactionState transaction();

    ConnectionStatsView connectionStats();

    int respVersion();

    void setRespVersion(int respVersion);
}
