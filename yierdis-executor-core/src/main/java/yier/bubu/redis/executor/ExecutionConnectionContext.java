package yier.bubu.redis.executor;

import yier.bubu.redis.contract.ServerSession;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class ExecutionConnectionContext {
    private final DefaultExecutionSession session;
    private final QueueState queueState = new QueueState();
    private final AtomicInteger pending = new AtomicInteger(0);
    private final AtomicLong pendingBytes = new AtomicLong(0);
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final AtomicBoolean inputDisabledByExecutor = new AtomicBoolean(false);
    private final AtomicLong commandsEnqueued = new AtomicLong(0);
    private final AtomicLong commandsExecuted = new AtomicLong(0);
    private final AtomicLong commandsRejected = new AtomicLong(0);
    private final AtomicLong commandsSkippedClosing = new AtomicLong(0);
    private final AtomicLong closeAfterReply = new AtomicLong(0);
    private final AtomicLong backpressureEnter = new AtomicLong(0);
    private final AtomicLong backpressureExit = new AtomicLong(0);

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
        return pending.get();
    }

    public long pendingBytes() {
        return pendingBytes.get();
    }

    public boolean isClosing() {
        return closing.get();
    }

    public boolean markClosing() {
        if (!closing.compareAndSet(false, true)) {
            return false;
        }
        session.discardTransaction();
        return true;
    }

    public void recordCommandEnqueued(int retainedBytes) {
        pending.incrementAndGet();
        pendingBytes.addAndGet(Math.max(0, retainedBytes));
        commandsEnqueued.incrementAndGet();
    }

    public void recordCommandFinished(int retainedBytes, boolean executed) {
        pending.decrementAndGet();
        pendingBytes.addAndGet(-Math.max(0, retainedBytes));
        if (executed) {
            commandsExecuted.incrementAndGet();
        }
    }

    public void recordCommandRejected() {
        commandsRejected.incrementAndGet();
    }

    public void recordSkippedClosing() {
        commandsSkippedClosing.incrementAndGet();
    }

    public void recordCloseAfterReply() {
        closeAfterReply.incrementAndGet();
    }

    public void recordBackpressureEnter() {
        backpressureEnter.incrementAndGet();
    }

    public void recordBackpressureExit() {
        backpressureExit.incrementAndGet();
    }

    public boolean markInputDisabledByExecutor() {
        return inputDisabledByExecutor.compareAndSet(false, true);
    }

    public boolean autoReadDisabledByExecutor() {
        return inputDisabledByExecutor.get();
    }

    public boolean clearAutoReadDisabledByExecutor() {
        return inputDisabledByExecutor.compareAndSet(true, false);
    }

    public ConnectionStatsSnapshot statsSnapshot() {
        return new ConnectionStatsSnapshot(
                pending.get(),
                pendingBytes.get(),
                inputDisabledByExecutor.get(),
                closing.get(),
                commandsEnqueued.get(),
                commandsExecuted.get(),
                commandsRejected.get(),
                commandsSkippedClosing.get(),
                closeAfterReply.get(),
                backpressureEnter.get(),
                backpressureExit.get()
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
