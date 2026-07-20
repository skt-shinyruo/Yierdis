package yier.bubu.redis.execution.executor;

import yier.bubu.redis.execution.api.ConnectionStatsView;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class ExecutionConnectionContext {
    private final QueueState queueState = new QueueState();
    private final AtomicInteger pending = new AtomicInteger(0);
    private final AtomicLong pendingBytes = new AtomicLong(0);
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final AtomicBoolean inputDisabledByExecutor = new AtomicBoolean(false);
    private final AtomicBoolean inputPausedByReply = new AtomicBoolean(false);
    private final AtomicLong commandsEnqueued = new AtomicLong(0);
    private final AtomicLong commandsExecuted = new AtomicLong(0);
    private final AtomicLong commandsRejected = new AtomicLong(0);
    private final AtomicLong commandsSkippedClosing = new AtomicLong(0);
    private final AtomicLong closeAfterReply = new AtomicLong(0);
    private final AtomicLong backpressureEnter = new AtomicLong(0);
    private final AtomicLong backpressureExit = new AtomicLong(0);

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
        return closing.compareAndSet(false, true);
    }

    public void recordCommandEnqueued(int retainedBytes) {
        pending.incrementAndGet();
        pendingBytes.addAndGet(Math.max(0, retainedBytes));
        commandsEnqueued.incrementAndGet();
    }

    void rollbackCommandEnqueued(int retainedBytes) {
        pending.decrementAndGet();
        pendingBytes.addAndGet(-Math.max(0, retainedBytes));
        commandsEnqueued.decrementAndGet();
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

    public boolean markInputPausedByReply() {
        return inputPausedByReply.compareAndSet(false, true);
    }

    public boolean inputPausedByReply() {
        return inputPausedByReply.get();
    }

    public boolean clearInputPausedByReply() {
        return inputPausedByReply.compareAndSet(true, false);
    }

    public ConnectionStatsSnapshot statsSnapshot() {
        return new ConnectionStatsSnapshot(
                pending.get(),
                pendingBytes.get(),
                inputDisabledByExecutor.get(),
                inputPausedByReply.get(),
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
            boolean inputPausedByReply,
            boolean closing,
            long commandsEnqueued,
            long commandsExecuted,
            long commandsRejected,
            long commandsSkippedClosing,
            long closeAfterReply,
            long backpressureEnter,
            long backpressureExit
    ) implements ConnectionStatsView {
    }

    private static final class QueueState implements ExecutorKeyState<Object> {
        private final ConcurrentLinkedQueue<Object> queue = new ConcurrentLinkedQueue<>();
        private final AtomicBoolean scheduled = new AtomicBoolean(false);
        private final java.util.concurrent.atomic.AtomicReference<Object> blockedHead = new java.util.concurrent.atomic.AtomicReference<>();
        private final AtomicBoolean blockedHeadReady = new AtomicBoolean(false);

        @Override
        public Queue<Object> queue() {
            return queue;
        }

        @Override
        public AtomicBoolean scheduled() {
            return scheduled;
        }

        @Override
        public java.util.concurrent.atomic.AtomicReference<Object> blockedHead() {
            return blockedHead;
        }

        @Override
        public AtomicBoolean blockedHeadReady() {
            return blockedHeadReady;
        }
    }
}
