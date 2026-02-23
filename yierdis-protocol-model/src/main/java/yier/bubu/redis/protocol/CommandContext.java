package yier.bubu.redis.protocol;

import java.util.Objects;

/**
 * 命令执行上下文（协议无关）。
 * <p>
 * 将“输入侧状态（Session）”与“输出端口（ReplyWriter）”显式拆分并聚合到同一上下文对象中，
 * 避免把路由/事务/鉴权等输入侧决策挂在 ReplyWriter 上。
 * <p>
 * 线程语义：该对象通常由单线程执行器在 owner thread 内复用（通过 {@link #reset(Session, ReplyWriter)}），
 * 不应跨线程共享或缓存到命令执行之外的生命周期中。
 */
public final class CommandContext {
    private Session session;
    private ReplyWriter out;

    public CommandContext(Session session, ReplyWriter out) {
        this.session = session;
        this.out = Objects.requireNonNull(out, "out");
    }

    /**
     * 重置上下文以复用对象，减少 per-command 分配。
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
     * Best-effort：若当前 session 为 server-side 连接态则返回，否则返回 null。
     */
    public ServerSession serverSessionOrNull() {
        Session s = session;
        if (s instanceof ServerSession ss) {
            return ss;
        }
        return null;
    }

    /**
     * Best-effort：若当前 session 支持提供 DB index 则返回，否则返回 null。
     */
    public DbIndexProvider dbIndexProviderOrNull() {
        Session s = session;
        if (s instanceof DbIndexProvider p) {
            return p;
        }
        return null;
    }
}

