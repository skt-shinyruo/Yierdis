package yier.bubu.redis.execution.api;

import java.util.Objects;

/**
 * Narrow capability bundle required by command execution.
 */
public final class CommandSessionCapabilities {
    private static final String REQUIRED_CAPABILITIES = "command session capabilities";

    private final DbIndexSession dbIndexSession;
    private final ClientMetadataSession clientMetadataSession;
    private final TransactionSession transactionSession;
    private final ConnectionStatsSession connectionStatsSession;
    private final ProtocolNegotiationSession protocolNegotiationSession;

    private CommandSessionCapabilities(
            DbIndexSession dbIndexSession,
            ClientMetadataSession clientMetadataSession,
            TransactionSession transactionSession,
            ConnectionStatsSession connectionStatsSession,
            ProtocolNegotiationSession protocolNegotiationSession
    ) {
        this.dbIndexSession = Objects.requireNonNull(dbIndexSession, "dbIndexSession");
        this.clientMetadataSession = Objects.requireNonNull(clientMetadataSession, "clientMetadataSession");
        this.transactionSession = Objects.requireNonNull(transactionSession, "transactionSession");
        this.connectionStatsSession = Objects.requireNonNull(connectionStatsSession, "connectionStatsSession");
        this.protocolNegotiationSession = Objects.requireNonNull(protocolNegotiationSession, "protocolNegotiationSession");
    }

    public static CommandSessionCapabilities from(ServerSession session) {
        Objects.requireNonNull(session, "session");
        return new CommandSessionCapabilities(session, session, session, session, session);
    }

    public static CommandSessionCapabilities from(Session session) {
        if (!(session instanceof DbIndexSession dbIndexSession)
                || !(session instanceof ClientMetadataSession clientMetadataSession)
                || !(session instanceof TransactionSession transactionSession)
                || !(session instanceof ConnectionStatsSession connectionStatsSession)
                || !(session instanceof ProtocolNegotiationSession protocolNegotiationSession)) {
            throw new IllegalArgumentException("YierdisEngine requires " + REQUIRED_CAPABILITIES);
        }
        return new CommandSessionCapabilities(
                dbIndexSession,
                clientMetadataSession,
                transactionSession,
                connectionStatsSession,
                protocolNegotiationSession
        );
    }

    public static CommandSessionCapabilities of(
            DbIndexSession dbIndexSession,
            ClientMetadataSession clientMetadataSession,
            TransactionSession transactionSession,
            ConnectionStatsSession connectionStatsSession,
            ProtocolNegotiationSession protocolNegotiationSession
    ) {
        return new CommandSessionCapabilities(
                dbIndexSession,
                clientMetadataSession,
                transactionSession,
                connectionStatsSession,
                protocolNegotiationSession
        );
    }

    public DbIndexSession dbIndexSession() {
        return dbIndexSession;
    }

    public ClientMetadataSession clientMetadataSession() {
        return clientMetadataSession;
    }

    public TransactionSession transactionSession() {
        return transactionSession;
    }

    public ConnectionStatsSession connectionStatsSession() {
        return connectionStatsSession;
    }

    public ProtocolNegotiationSession protocolNegotiationSession() {
        return protocolNegotiationSession;
    }
}
