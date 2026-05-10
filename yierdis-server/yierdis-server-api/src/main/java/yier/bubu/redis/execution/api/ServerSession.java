package yier.bubu.redis.execution.api;

/**
 * Server-side per-connection session state exposed to the command layer.
 * <p>
 * This is transport-agnostic and models Redis-like connection state such as selected DB, client metadata, AUTH state,
 * MULTI transaction queue, and read-only connection stats.
 */
public interface ServerSession extends Session {
    int dbIndex();

    void setDbIndex(int dbIndex);

    long clientId();

    String clientName();

    void setClientName(String clientName);

    boolean authenticated();

    void setAuthenticated(boolean authenticated);

    TransactionState transaction();

    ConnectionStatsView connectionStats();

    default int respVersion() {
        return 2;
    }

    default void setRespVersion(int respVersion) {
        // no-op
    }
}
