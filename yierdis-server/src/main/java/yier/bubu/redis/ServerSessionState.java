package yier.bubu.redis;

// Server 会话状态（server 私有）：实现协议层 ServerSession，承载 SELECT/AUTH/MULTI 等 Redis-like 连接态。

import yier.bubu.redis.contract.ServerSession;
import yier.bubu.redis.contract.TransactionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

final class ServerSessionState implements ServerSession {
    private static final AtomicLong NEXT_CLIENT_ID = new AtomicLong(1);
    static final int DEFAULT_TRANSACTION_QUEUE_MAX_COMMANDS = 1024;
    static final long DEFAULT_TRANSACTION_QUEUE_MAX_BYTES = 64L * 1024 * 1024; // 64 MiB

    // --- Redis-like connection state（通过 ServerSession 暴露给命令层） ---
    private final long clientId = NEXT_CLIENT_ID.getAndIncrement();
    private int dbIndex;
    private String clientName;
    private boolean authenticated;
    private final ConnectionTransactionState transaction;

    private final ServerRuntimeState runtime;

    ServerSessionState(ServerRuntimeState runtime, int transactionQueueMaxCommands, long transactionQueueMaxBytes) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.transaction = new ConnectionTransactionState(transactionQueueMaxCommands, transactionQueueMaxBytes);
    }

    ServerRuntimeState runtime() {
        return runtime;
    }

    ServerConnectionContext.ConnectionStatsSnapshot connectionStatsSnapshot() {
        return new ServerConnectionContext.ConnectionStatsSnapshot(
                runtime.pendingCounter().get(),
                runtime.pendingBytesCounter().get(),
                runtime.autoReadDisabledByExecutor(),
                runtime.isClosing(),
                runtime.commandsEnqueuedCounter().get(),
                runtime.commandsExecutedCounter().get(),
                runtime.commandsRejectedCounter().get(),
                runtime.commandsSkippedClosingCounter().get(),
                runtime.closeAfterReplyCounter().get(),
                runtime.backpressureEnterCounter().get(),
                runtime.backpressureExitCounter().get()
        );
    }

    void discardTransaction() {
        transaction.discard();
    }

    @Override
    public int dbIndex() {
        return dbIndex;
    }

    @Override
    public void setDbIndex(int dbIndex) {
        this.dbIndex = Math.max(0, dbIndex);
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
        return authenticated;
    }

    @Override
    public void setAuthenticated(boolean authenticated) {
        this.authenticated = authenticated;
    }

    @Override
    public TransactionState transaction() {
        return transaction;
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
