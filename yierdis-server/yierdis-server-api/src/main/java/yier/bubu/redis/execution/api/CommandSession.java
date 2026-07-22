package yier.bubu.redis.execution.api;

public interface CommandSession extends
        DbIndexSession,
        ClientMetadataSession,
        TransactionSession,
        ConnectionStatsSession,
        ProtocolNegotiationSession {
}
