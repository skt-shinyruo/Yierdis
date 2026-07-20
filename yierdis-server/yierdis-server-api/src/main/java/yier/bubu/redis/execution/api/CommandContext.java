package yier.bubu.redis.execution.api;

import java.util.Objects;
import yier.bubu.redis.common.command.MutationContext;

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
    private MutationContext mutationContext;

    public CommandContext(CommandSessionCapabilities session, RedisReplyWriter out) {
        this(session, out, MutationContext.none());
    }

    public CommandContext(
            CommandSessionCapabilities session,
            RedisReplyWriter out,
            MutationContext mutationContext
    ) {
        this.session = Objects.requireNonNull(session, "session");
        this.out = Objects.requireNonNull(out, "out");
        this.mutationContext = Objects.requireNonNull(mutationContext, "mutationContext");
    }

    public CommandContext reset(CommandSessionCapabilities session, RedisReplyWriter out) {
        return reset(session, out, MutationContext.none());
    }

    public CommandContext reset(
            CommandSessionCapabilities session,
            RedisReplyWriter out,
            MutationContext mutationContext
    ) {
        this.session = Objects.requireNonNull(session, "session");
        this.out = Objects.requireNonNull(out, "out");
        this.mutationContext = Objects.requireNonNull(mutationContext, "mutationContext");
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

    public MutationContext mutationContext() {
        return mutationContext;
    }

    /**
     * 结束当前命令对 mutation 元数据的借用。
     */
    public void releaseMutationContext() {
        mutationContext.close();
        mutationContext = MutationContext.none();
    }

}
