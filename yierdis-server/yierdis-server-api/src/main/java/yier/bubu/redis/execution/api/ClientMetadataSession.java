package yier.bubu.redis.execution.api;

/**
 * Connection-scoped client metadata and AUTH state.
 */
public interface ClientMetadataSession {
    long clientId();

    String clientName();

    void setClientName(String clientName);

    boolean authenticated();

    void setAuthenticated(boolean authenticated);
}
