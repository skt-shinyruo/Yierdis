package yier.bubu.redis.contract;

import java.util.Objects;

/**
 * Command execution context (transport-agnostic).
 * <p>
 * Groups the required server-side session and the output port. The command path must not silently run with a weaker
 * marker session because DB routing, transactions, connection metadata, and change emission all depend on explicit
 * server session semantics.
 */
public final class CommandContext {
    private ServerSession session;
    private ReplyWriter out;
    private boolean valueChanged;
    private boolean ttlChanged;

    public CommandContext(ServerSession session, ReplyWriter out) {
        this.session = Objects.requireNonNull(session, "session");
        this.out = Objects.requireNonNull(out, "out");
    }

    public CommandContext reset(ServerSession session, ReplyWriter out) {
        this.session = Objects.requireNonNull(session, "session");
        this.out = Objects.requireNonNull(out, "out");
        clearMutationOutcome();
        return this;
    }

    public ServerSession session() {
        return session;
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
