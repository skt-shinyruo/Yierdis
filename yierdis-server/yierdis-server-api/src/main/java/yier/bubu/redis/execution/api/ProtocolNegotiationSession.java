package yier.bubu.redis.execution.api;

/**
 * Connection-scoped RESP protocol negotiation state.
 */
public interface ProtocolNegotiationSession extends Session {
    default int respVersion() {
        return 2;
    }

    default void setRespVersion(int respVersion) {
        // no-op
    }
}
