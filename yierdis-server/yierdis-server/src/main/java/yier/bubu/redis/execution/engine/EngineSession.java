package yier.bubu.redis.execution.engine;

import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ConnectionStatsView;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.TransactionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 与一条执行连接绑定的命令会话状态。
 *
 * <p>实例持有 DB 选择、客户端元数据、认证状态、RESP 版本以及事务队列中 retain 的请求。
 * 连接关闭路径必须调用 {@link #discardTransaction()} 归还尚未重放的请求；生产组装会优先在 command owner
 * 上执行该清理，并在 owner 已退出时同步兜底。连接统计由外部 supplier 提供，不属于本实例拥有的状态。</p>
 */
public final class EngineSession implements CommandSession {
    private final DefaultTransactionState transaction;
    private volatile Supplier<ConnectionStatsView> connectionStatsSupplier = () -> null;
    private int dbIndex;
    private String clientName;
    private volatile int respVersion = 2;

    public EngineSession(int maxQueuedCommands, long maxQueuedBytes) {
        this.transaction = new DefaultTransactionState(maxQueuedCommands, maxQueuedBytes);
    }

    public void discardTransaction() {
        transaction.discard();
    }

    /**
     * Binds a read-only stats supplier owned by the transport/executor connection.
     * The session does not own these counters; it only exposes them to INFO/STATS.
     */
    public void bindConnectionStatsSupplier(Supplier<ConnectionStatsView> supplier) {
        connectionStatsSupplier = supplier == null ? () -> null : supplier;
    }

    @Override
    public ConnectionStatsView connectionStats() {
        return connectionStatsSupplier.get();
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
    public int respVersion() {
        return respVersion;
    }

    @Override
    public void setRespVersion(int respVersion) {
        if (respVersion != 2 && respVersion != 3) {
            throw new IllegalArgumentException("NOPROTO unsupported protocol version");
        }
        this.respVersion = respVersion;
    }

    @Override
    public TransactionState transaction() {
        return transaction;
    }

    private static final class DefaultTransactionState implements TransactionState {
        private final int maxQueuedCommands;
        private final long maxQueuedBytes;
        private final ArrayList<ExecutionRequest> queue = new ArrayList<>();
        private boolean active;
        private boolean aborted;
        private long queuedBytes;

        private DefaultTransactionState(int maxQueuedCommands, long maxQueuedBytes) {
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
            closeOwnedRequests();
            active = true;
            aborted = false;
            queuedBytes = 0;
        }

        @Override
        public synchronized void discard() {
            closeOwnedRequests();
            active = false;
            aborted = false;
            queuedBytes = 0;
        }

        @Override
        public synchronized String tryEnqueue(ExecutionRequest request) {
            if (request == null) {
                return null;
            }
            if (maxQueuedCommands > 0 && queue.size() >= maxQueuedCommands) {
                aborted = true;
                return "ERR Transaction queue is full";
            }

            long estimatedBytes = Math.max(0L, request.retainedBytes());
            if (exceedsQueuedBytesLimit(estimatedBytes)) {
                aborted = true;
                return "ERR Transaction queue is full";
            }

            ExecutionRequest retained = request.retain();
            long requestBytes = Math.max(0L, retained.retainedBytes());
            if (exceedsQueuedBytesLimit(requestBytes)) {
                aborted = true;
                retained.close();
                return "ERR Transaction queue is full";
            }

            queue.add(retained);
            queuedBytes += requestBytes;
            return null;
        }

        private boolean exceedsQueuedBytesLimit(long requestBytes) {
            return maxQueuedBytes > 0
                    && requestBytes > 0L
                    && (requestBytes > maxQueuedBytes || queuedBytes > maxQueuedBytes - requestBytes);
        }

        @Override
        public synchronized int size() {
            return queue.size();
        }

        @Override
        public synchronized void forEachQueued(Consumer<? super ExecutionRequest> visitor) {
            Objects.requireNonNull(visitor, "visitor");
            for (ExecutionRequest request : queue) {
                visitor.accept(request);
            }
        }

        @Override
        public synchronized List<ExecutionRequest> drain() {
            ArrayList<ExecutionRequest> out = new ArrayList<>(queue);
            queue.clear();
            active = false;
            aborted = false;
            queuedBytes = 0;
            return out;
        }

        private void closeOwnedRequests() {
            for (ExecutionRequest request : queue) {
                if (request == null) {
                    continue;
                }
                try {
                    request.close();
                } catch (Throwable ignored) {
                    // Ignore cleanup failures while resetting transaction state.
                }
            }
            queue.clear();
        }

    }
}
