package yier.bubu.redis.executor;

import yier.bubu.redis.contract.ServerSession;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ExecutionConnectionContext {
    private final DefaultExecutionSession session;
    private final QueueState queueState = new QueueState();
    private int pending;
    private long pendingBytes;
    private boolean closing;
    private boolean inputDisabledByExecutor;
    private long commandsEnqueued;
    private long commandsExecuted;
    private long commandsRejected;
    private long commandsSkippedClosing;
    private long closeAfterReply;
    private long backpressureEnter;
    private long backpressureExit;

    public ExecutionConnectionContext(DefaultExecutionSession session) {
        this.session = session;
        this.session.attach(this);
    }

    public ServerSession session() {
        return session;
    }

    public ExecutorKeyState<Object> queueState() {
        return queueState;
    }

    public int pending() {
        return pending;
    }

    public long pendingBytes() {
        return pendingBytes;
    }

    public boolean isClosing() {
        return closing;
    }

    public boolean markClosing() {
        if (closing) {
            return false;
        }
        closing = true;
        session.discardTransaction();
        return true;
    }

    public void recordCommandEnqueued(int retainedBytes) {
        pending++;
        pendingBytes += Math.max(0, retainedBytes);
        commandsEnqueued++;
    }

    public void recordCommandFinished(int retainedBytes, boolean executed) {
        pending--;
        pendingBytes -= Math.max(0, retainedBytes);
        if (executed) {
            commandsExecuted++;
        }
    }

    public void recordCommandRejected() {
        commandsRejected++;
    }

    public void recordSkippedClosing() {
        commandsSkippedClosing++;
    }

    public void recordCloseAfterReply() {
        closeAfterReply++;
    }

    public void recordBackpressureEnter() {
        backpressureEnter++;
    }

    public void recordBackpressureExit() {
        backpressureExit++;
    }

    public boolean markInputDisabledByExecutor() {
        if (inputDisabledByExecutor) {
            return false;
        }
        inputDisabledByExecutor = true;
        return true;
    }

    public boolean autoReadDisabledByExecutor() {
        return inputDisabledByExecutor;
    }

    public boolean clearAutoReadDisabledByExecutor() {
        if (!inputDisabledByExecutor) {
            return false;
        }
        inputDisabledByExecutor = false;
        return true;
    }

    public ConnectionStatsSnapshot statsSnapshot() {
        return new ConnectionStatsSnapshot(
                pending,
                pendingBytes,
                inputDisabledByExecutor,
                closing,
                commandsEnqueued,
                commandsExecuted,
                commandsRejected,
                commandsSkippedClosing,
                closeAfterReply,
                backpressureEnter,
                backpressureExit
        );
    }

    public record ConnectionStatsSnapshot(
            int pending,
            long pendingBytes,
            boolean inputDisabledByExecutor,
            boolean closing,
            long commandsEnqueued,
            long commandsExecuted,
            long commandsRejected,
            long commandsSkippedClosing,
            long closeAfterReply,
            long backpressureEnter,
            long backpressureExit
    ) {
    }

    private static final class QueueState implements ExecutorKeyState<Object> {
        private final ConcurrentLinkedQueue<Object> queue = new ConcurrentLinkedQueue<>();
        private final AtomicBoolean scheduled = new AtomicBoolean(false);

        @Override
        public Queue<Object> queue() {
            return queue;
        }

        @Override
        public AtomicBoolean scheduled() {
            return scheduled;
        }
    }
}
