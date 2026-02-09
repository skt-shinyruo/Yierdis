package yier.bubu.redis.protocol;

/**
 * Server-side per-connection session state exposed to the command layer.
 * <p>
 * This is protocol-agnostic and intentionally minimal: it models Redis-like connection state such as SELECTed DB,
 * AUTH state and MULTI transaction queue.
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
}

