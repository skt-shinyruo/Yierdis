package yier.bubu.redis.execution.api;

import java.util.Objects;

/**
 * Command execution context (transport-agnostic).
 * <p>
 * Groups the required server-side session and the output port. The command path must not silently run with a weaker
 * marker session because DB routing, transactions, connection metadata, and change emission all depend on explicit
 * server session semantics.
 */
public final class CommandContext {
    private CommandSessionCapabilities session;
    private ReplyWriter out;
    private boolean valueChanged;
    private boolean ttlChanged;

    public CommandContext(ServerSession session, ReplyWriter out) {
        this(CommandSessionCapabilities.from(session), out);
    }

    public CommandContext(CommandSessionCapabilities session, ReplyWriter out) {
        this.session = Objects.requireNonNull(session, "session");
        this.out = Objects.requireNonNull(out, "out");
    }

    public CommandContext reset(ServerSession session, ReplyWriter out) {
        return reset(CommandSessionCapabilities.from(session), out);
    }

    public CommandContext reset(CommandSessionCapabilities session, ReplyWriter out) {
        this.session = Objects.requireNonNull(session, "session");
        this.out = Objects.requireNonNull(out, "out");
        clearMutationOutcome();
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

    public ReplyWriter out() {
        return out;
    }

    public void clearMutationOutcome() {
        valueChanged = false;
        ttlChanged = false;
    }

    public void recordMutation(boolean changedValue, boolean changedTtl) {
        valueChanged |= changedValue;
        ttlChanged |= changedTtl;
    }

    public boolean valueChanged() {
        return valueChanged;
    }

    public boolean ttlChanged() {
        return ttlChanged;
    }

    public boolean changedAny() {
        return valueChanged || ttlChanged;
    }
}
