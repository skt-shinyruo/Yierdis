package yier.bubu.redis.contract;

import java.util.Objects;

/**
 * Command execution context (transport-agnostic).
 * <p>
 * Groups the "input-side state" (Session) and the output port (ReplyWriter) into a single context object, avoiding
 * leaking routing/transaction/auth decisions into the output layer.
 * <p>
 * Threading: typically reused by a single-thread executor via {@link #reset(Session, ReplyWriter)} and must not be
 * shared across threads.
 */
public final class CommandContext {
    private Session session;
    private ReplyWriter out;

    public CommandContext(Session session, ReplyWriter out) {
        this.session = session;
        this.out = Objects.requireNonNull(out, "out");
    }

    /**
     * Resets the context for object reuse to reduce per-command allocations.
     */
    public CommandContext reset(Session session, ReplyWriter out) {
        this.session = session;
        this.out = Objects.requireNonNull(out, "out");
        return this;
    }

    public Session session() {
        return session;
    }

    public ReplyWriter out() {
        return out;
    }

    /**
     * Best-effort: returns server-side session state when available, otherwise null.
     */
    public ServerSession serverSessionOrNull() {
        Session s = session;
        if (s instanceof ServerSession ss) {
            return ss;
        }
        return null;
    }

    /**
     * Best-effort: returns a DB index provider when available, otherwise null.
     */
    public DbIndexProvider dbIndexProviderOrNull() {
        Session s = session;
        if (s instanceof DbIndexProvider p) {
            return p;
        }
        return null;
    }
}

