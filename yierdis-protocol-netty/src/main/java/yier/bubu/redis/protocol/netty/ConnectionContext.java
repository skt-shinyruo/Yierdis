package yier.bubu.redis.protocol.netty;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import yier.bubu.redis.protocol.RespProtocol;
import yier.bubu.redis.protocol.RespSession;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 连接级上下文（SSOT）。
 * <p>
 * 该对象用于替代散落在多个 {@link Channel#attr(AttributeKey)} 中的状态：
 * - RESP 协议协商（RESP2/RESP3）
 * - 执行器的 per-connection pending/backpressure/closing 状态
 * - 为可观测性预留的连接级统计容器
 */
public final class ConnectionContext implements RespSession {
    private static final AttributeKey<ConnectionContext> KEY =
            AttributeKey.valueOf("yierdis.connectionContext");

    public static ConnectionContext getOrCreate(Channel channel) {
        Objects.requireNonNull(channel, "channel");
        Attribute<ConnectionContext> attr = channel.attr(KEY);
        ConnectionContext existing = attr.get();
        if (existing != null) {
            return existing;
        }
        ConnectionContext created = new ConnectionContext();
        ConnectionContext raced = attr.setIfAbsent(created);
        return raced == null ? created : raced;
    }

    // --- RESP session ---
    private volatile RespProtocol protocol = RespProtocol.RESP2;

    // --- Executor / backpressure (跨线程读写，使用原子类型) ---
    private final AtomicInteger pending = new AtomicInteger(0);
    private final AtomicLong pendingBytes = new AtomicLong(0);
    private final AtomicBoolean autoReadDisabledByExecutor = new AtomicBoolean(false);
    private final AtomicBoolean closing = new AtomicBoolean(false);

    // FAIR 调度的 per-channel 队列状态（队列元素类型由 server/executor 决定）。
    public static final class ExecutorState {
        private final ConcurrentLinkedQueue<Object> queue = new ConcurrentLinkedQueue<>();
        private final AtomicBoolean scheduled = new AtomicBoolean(false);

        public ConcurrentLinkedQueue<Object> queue() {
            return queue;
        }

        public AtomicBoolean scheduled() {
            return scheduled;
        }
    }

    private final ExecutorState executorState = new ExecutorState();

    // --- Observability（低开销容器，具体字段在 server 侧填充/使用） ---
    private final AtomicLong commandsEnqueued = new AtomicLong(0);
    private final AtomicLong commandsExecuted = new AtomicLong(0);
    private final AtomicLong commandsRejected = new AtomicLong(0);
    private final AtomicLong commandsSkippedClosing = new AtomicLong(0);
    private final AtomicLong closeAfterReply = new AtomicLong(0);
    private final AtomicLong backpressureEnter = new AtomicLong(0);
    private final AtomicLong backpressureExit = new AtomicLong(0);

    private ConnectionContext() {
    }

    @Override
    public RespProtocol protocol() {
        RespProtocol p = protocol;
        return p == null ? RespProtocol.RESP2 : p;
    }

    @Override
    public void setProtocol(RespProtocol protocol) {
        this.protocol = protocol == null ? RespProtocol.RESP2 : protocol;
    }

    public AtomicInteger pendingCounter() {
        return pending;
    }

    public AtomicLong pendingBytesCounter() {
        return pendingBytes;
    }

    public boolean markAutoReadDisabledByExecutor() {
        return autoReadDisabledByExecutor.compareAndSet(false, true);
    }

    public boolean clearAutoReadDisabledByExecutor() {
        return autoReadDisabledByExecutor.compareAndSet(true, false);
    }

    public boolean autoReadDisabledByExecutor() {
        return autoReadDisabledByExecutor.get();
    }

    public boolean isClosing() {
        return closing.get();
    }

    public void markClosing() {
        closing.set(true);
    }

    public ExecutorState executorState() {
        return executorState;
    }

    public AtomicLong commandsEnqueuedCounter() {
        return commandsEnqueued;
    }

    public AtomicLong commandsExecutedCounter() {
        return commandsExecuted;
    }

    public AtomicLong commandsRejectedCounter() {
        return commandsRejected;
    }

    public AtomicLong commandsSkippedClosingCounter() {
        return commandsSkippedClosing;
    }

    public AtomicLong closeAfterReplyCounter() {
        return closeAfterReply;
    }

    public AtomicLong backpressureEnterCounter() {
        return backpressureEnter;
    }

    public AtomicLong backpressureExitCounter() {
        return backpressureExit;
    }
}
