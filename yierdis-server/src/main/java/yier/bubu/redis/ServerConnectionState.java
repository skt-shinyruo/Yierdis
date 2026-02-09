package yier.bubu.redis;

// Server 连接运行时状态（server 私有）：承载背压/计数/closing 等语义，并暴露最小 Redis-like 连接态给命令层。

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import yier.bubu.redis.protocol.ServerSession;
import yier.bubu.redis.protocol.TransactionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Server-side per-connection runtime state.
 * <p>
 * 该对象位于 server 模块中，用于承载与 Netty 执行器相关的运行时语义：
 * pending/backpressure/closing/counters 等。
 */
final class ServerConnectionState implements ServerSession {
    private static final AttributeKey<ServerConnectionState> KEY =
            AttributeKey.valueOf("yierdis.serverConnectionState");
    private static final java.util.concurrent.atomic.AtomicLong NEXT_CLIENT_ID = new java.util.concurrent.atomic.AtomicLong(1);
    private static final int DEFAULT_TRANSACTION_QUEUE_MAX_COMMANDS = 1024;
    private static final long DEFAULT_TRANSACTION_QUEUE_MAX_BYTES = 64L * 1024 * 1024; // 64 MiB

    static ServerConnectionState getOrCreate(Channel channel) {
        return getOrCreate(channel, DEFAULT_TRANSACTION_QUEUE_MAX_COMMANDS, DEFAULT_TRANSACTION_QUEUE_MAX_BYTES);
    }

    static ServerConnectionState getOrCreate(
            Channel channel,
            int transactionQueueMaxCommands,
            long transactionQueueMaxBytes
    ) {
        Objects.requireNonNull(channel, "channel");
        Attribute<ServerConnectionState> attr = channel.attr(KEY);
        ServerConnectionState existing = attr.get();
        if (existing != null) {
            return existing;
        }
        ServerConnectionState created = new ServerConnectionState(
                Math.max(0, transactionQueueMaxCommands),
                Math.max(0, transactionQueueMaxBytes)
        );
        ServerConnectionState raced = attr.setIfAbsent(created);
        if (raced == null) {
            return created;
        }
        return raced;
    }

    // --- Redis-like connection state（跨模块可见，通过 ServerSession 暴露给命令层） ---
    private final long clientId = NEXT_CLIENT_ID.getAndIncrement();
    private final AtomicInteger dbIndex = new AtomicInteger(0);
    private volatile String clientName;
    private final AtomicBoolean authenticated = new AtomicBoolean(false);
    private final ConnectionTransactionState transaction;

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

    private ServerConnectionState(int transactionQueueMaxCommands, long transactionQueueMaxBytes) {
        this.transaction = new ConnectionTransactionState(transactionQueueMaxCommands, transactionQueueMaxBytes);
    }

    @Override
    public int dbIndex() {
        return dbIndex.get();
    }

    @Override
    public void setDbIndex(int dbIndex) {
        this.dbIndex.set(Math.max(0, dbIndex));
    }

    @Override
    public long clientId() {
        return clientId;
    }

    @Override
    public String clientName() {
        return clientName;
    }

    @Override
    public void setClientName(String clientName) {
        String name = clientName;
        if (name != null) {
            name = name.trim();
            if (name.isEmpty()) {
                name = null;
            }
        }
        this.clientName = name;
    }

    @Override
    public boolean authenticated() {
        return authenticated.get();
    }

    @Override
    public void setAuthenticated(boolean authenticated) {
        this.authenticated.set(authenticated);
    }

    @Override
    public TransactionState transaction() {
        return transaction;
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
        // 连接关闭时清理连接态资源，避免悬挂引用/内存驻留。
        transaction.discard();
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

    private static final class ConnectionTransactionState implements TransactionState {
        private final int maxQueuedCommands;
        private final long maxQueuedBytes;
        private boolean active;
        private boolean aborted;
        private long queuedBytes;
        private final ArrayList<byte[][]> queue = new ArrayList<>();

        private ConnectionTransactionState(int maxQueuedCommands, long maxQueuedBytes) {
            this.maxQueuedCommands = Math.max(0, maxQueuedCommands);
            this.maxQueuedBytes = Math.max(0, maxQueuedBytes);
        }

        @Override
        public synchronized boolean active() {
            return active;
        }

        @Override
        public synchronized boolean aborted() {
            return aborted;
        }

        @Override
        public synchronized void markAborted() {
            aborted = true;
        }

        @Override
        public synchronized void begin() {
            active = true;
            aborted = false;
            queuedBytes = 0;
            queue.clear();
        }

        @Override
        public synchronized void discard() {
            active = false;
            aborted = false;
            queuedBytes = 0;
            queue.clear();
        }

        @Override
        public synchronized void enqueue(byte[][] argv) {
            tryEnqueue(argv);
        }

        @Override
        public synchronized String tryEnqueue(byte[][] argv) {
            if (argv == null) {
                return null;
            }
            if (maxQueuedCommands > 0 && queue.size() >= maxQueuedCommands) {
                aborted = true;
                return "ERR Transaction queue is full";
            }

            long argvBytes = estimateArgvBytes(argv);
            if (maxQueuedBytes > 0 && queuedBytes + argvBytes > maxQueuedBytes) {
                aborted = true;
                return "ERR Transaction queue is full";
            }

            queue.add(argv);
            queuedBytes += argvBytes;
            return null;
        }

        @Override
        public synchronized int size() {
            return queue.size();
        }

        @Override
        public synchronized List<byte[][]> drain() {
            ArrayList<byte[][]> out = new ArrayList<>(queue);
            queue.clear();
            active = false;
            aborted = false;
            queuedBytes = 0;
            return out;
        }

        private static long estimateArgvBytes(byte[][] argv) {
            if (argv == null) {
                return 0;
            }
            long total = 0;
            for (int i = 0; i < argv.length; i++) {
                byte[] arg = argv[i];
                if (arg != null) {
                    total += arg.length;
                }
            }
            return total;
        }
    }
}
