package yier.bubu.redis.execution.api;

public interface ProtocolNegotiationSession {
    int respVersion();

    void setRespVersion(int respVersion);
}
