package yier.bubu.redis.execution.api;

import java.util.Objects;

/**
 * Command execution context (transport-agnostic).
 * <p>
 * Groups the required server-side session and the output port. The command path must not silently run with a weaker
 * marker session because DB routing, transactions, and connection metadata all depend on explicit
 * server session semantics.
 */
public final class CommandContext {
    private CommandSessionCapabilities session;
    private RedisReplyWriter out;

    public CommandContext(CommandSessionCapabilities session, RedisReplyWriter out) {
        this.session = Objects.requireNonNull(session, "session");
        this.out = Objects.requireNonNull(out, "out");
    }

    public CommandContext reset(CommandSessionCapabilities session, RedisReplyWriter out) {
        this.session = Objects.requireNonNull(session, "session");
        this.out = Objects.requireNonNull(out, "out");
        return this;
    }

    public CommandSessionCapabilities sessionCapabilities() {
        return session;
    }

    public DbIndexSession dbIndexSession() {
        return session.dbIndexSession();
    }

    public ClientMetadataSession clientMetadataSession() {
        return session.clientMetadataSession();
    }

    public TransactionSession transactionSession() {
        return session.transactionSession();
    }

    public ConnectionStatsSession connectionStatsSession() {
        return session.connectionStatsSession();
    }

    public ProtocolNegotiationSession protocolNegotiationSession() {
        return session.protocolNegotiationSession();
    }

    public RedisReplyWriter out() {
        return out;
    }

}
