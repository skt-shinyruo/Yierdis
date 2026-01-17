package yier.bubu.redis;

// Server 连接运行时状态（server 私有）：承载背压/计数/closing 等语义，并通过 RespSession 代理协议协商状态。

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import yier.bubu.redis.protocol.RespProtocol;
import yier.bubu.redis.protocol.RespSession;
import yier.bubu.redis.protocol.netty.ConnectionContext;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Server-side per-connection runtime state.
 * <p>
 * 该对象位于 server 模块中，用于承载与 Netty 执行器相关的运行时语义：
 * pending/backpressure/closing/counters 等。协议协商状态（RESP2/RESP3）通过委托 {@link RespSession}
 * 获取与设置，避免 protocol-netty adapter 携带 server 语义。
 */
final class ServerConnectionState implements RespSession {
    private static final AttributeKey<ServerConnectionState> KEY =
            AttributeKey.valueOf("yierdis.serverConnectionState");

    static ServerConnectionState getOrCreate(Channel channel) {
        Objects.requireNonNull(channel, "channel");
        ConnectionContext session = ConnectionContext.getOrCreate(channel);
        return getOrCreate(channel, session);
    }

    static ServerConnectionState getOrCreate(Channel channel, RespSession session) {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(session, "session");
        Attribute<ServerConnectionState> attr = channel.attr(KEY);
        ServerConnectionState existing = attr.get();
        if (existing != null) {
            existing.bindSessionIfAbsent(session);
            return existing;
        }
        ServerConnectionState created = new ServerConnectionState(session);
        ServerConnectionState raced = attr.setIfAbsent(created);
        if (raced == null) {
            return created;
        }
        raced.bindSessionIfAbsent(session);
        return raced;
    }

    private volatile RespSession session;

    // --- Executor / backpressure (跨线程读写，使用原子类型) ---
    private final AtomicInteger pending = new AtomicInteger(0);
    private final AtomicLong pendingBytes = new AtomicLong(0);
    private final AtomicBoolean autoReadDisabledByExecutor = new AtomicBoolean(false);
    private final AtomicBoolean closing = new AtomicBoolean(false);

    // --- Observability（低开销容器，具体字段在 server 侧填充/使用） ---
    private final AtomicLong commandsEnqueued = new AtomicLong(0);
    private final AtomicLong commandsExecuted = new AtomicLong(0);
    private final AtomicLong commandsRejected = new AtomicLong(0);
    private final AtomicLong commandsSkippedClosing = new AtomicLong(0);
    private final AtomicLong closeAfterReply = new AtomicLong(0);
    private final AtomicLong backpressureEnter = new AtomicLong(0);
    private final AtomicLong backpressureExit = new AtomicLong(0);

    private ServerConnectionState(RespSession session) {
        this.session = Objects.requireNonNull(session, "session");
    }

    private void bindSessionIfAbsent(RespSession session) {
        if (this.session != null) {
            return;
        }
        this.session = Objects.requireNonNull(session, "session");
    }

    @Override
    public RespProtocol protocol() {
        RespSession s = session;
        if (s == null) {
            return RespProtocol.RESP2;
        }
        return s.protocol();
    }

    @Override
    public void setProtocol(RespProtocol protocol) {
        RespSession s = session;
        if (s == null) {
            return;
        }
        s.setProtocol(protocol);
    }

    AtomicInteger pendingCounter() {
        return pending;
    }

    AtomicLong pendingBytesCounter() {
        return pendingBytes;
    }

    boolean markAutoReadDisabledByExecutor() {
        return autoReadDisabledByExecutor.compareAndSet(false, true);
    }

    boolean clearAutoReadDisabledByExecutor() {
        return autoReadDisabledByExecutor.compareAndSet(true, false);
    }

    boolean autoReadDisabledByExecutor() {
        return autoReadDisabledByExecutor.get();
    }

    boolean isClosing() {
        return closing.get();
    }

    void markClosing() {
        closing.set(true);
    }

    AtomicLong commandsEnqueuedCounter() {
        return commandsEnqueued;
    }

    AtomicLong commandsExecutedCounter() {
        return commandsExecuted;
    }

    AtomicLong commandsRejectedCounter() {
        return commandsRejected;
    }

    AtomicLong commandsSkippedClosingCounter() {
        return commandsSkippedClosing;
    }

    AtomicLong closeAfterReplyCounter() {
        return closeAfterReply;
    }

    AtomicLong backpressureEnterCounter() {
        return backpressureEnter;
    }

    AtomicLong backpressureExitCounter() {
        return backpressureExit;
    }
}

