package yier.bubu.redis.engine;

import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.contract.ConnectionStatsProvider;
import yier.bubu.redis.contract.ConnectionStatsView;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ServerSession;
import yier.bubu.redis.contract.TransactionState;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Engine-owned per-connection command session state.
 */
public final class EngineSession implements ServerSession, ConnectionStatsProvider {
    private static final AtomicLong NEXT_CLIENT_ID = new AtomicLong(1);

    private final long clientId = NEXT_CLIENT_ID.getAndIncrement();
    private final DefaultTransactionState transaction;
    private volatile Supplier<ConnectionStatsView> connectionStatsSupplier = () -> null;
    private int dbIndex;
    private String clientName;
    private boolean authenticated;

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
        public synchronized void enqueue(ExecutionRequest request) {
            tryEnqueue(request);
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
            if (maxQueuedBytes > 0 && estimatedBytes > 0 && queuedBytes + estimatedBytes > maxQueuedBytes) {
                aborted = true;
                return "ERR Transaction queue is full";
            }

            ExecutionRequest snapshot = ByteArrayExecutionRequest.copyOf(request);
            long requestBytes = Math.max(0L, snapshot.retainedBytes());
            if (maxQueuedBytes > 0 && queuedBytes + requestBytes > maxQueuedBytes) {
                aborted = true;
                snapshot.close();
                return "ERR Transaction queue is full";
            }

            queue.add(snapshot);
            queuedBytes += requestBytes;
            return null;
        }

        @Override
        public synchronized int size() {
            return queue.size();
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
