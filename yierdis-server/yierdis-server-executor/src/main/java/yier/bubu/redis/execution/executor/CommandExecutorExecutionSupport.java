package yier.bubu.redis.execution.executor;

import yier.bubu.redis.execution.api.ReplyWriter;
import yier.bubu.redis.execution.api.ReplyWriterFactory;
import yier.bubu.redis.execution.api.ServerSession;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;

final class CommandExecutorExecutionSupport<C extends ExecutionConnection> {
    private final CommandExecutionEngine commandProcessor;
    private final ReplyWriterFactory replyWriterFactory;
    private final ExecutionIoAdapter<C> ioAdapter;
    private final ExecutorBacklogBudget backlogBudget;
    private final ExecutorBackpressureController<C> backpressureController;
    private final int backpressureLowWatermark;
    private final long backpressureBytesHighWatermark;
    private final long backpressureBytesLowWatermark;
    private final BooleanSupplier running;
    private final LongAdder commandsExecuted = new LongAdder();
    private final LongAdder commandsSkippedClosing = new LongAdder();
    private final LongAdder closeAfterReply = new LongAdder();

    CommandExecutorExecutionSupport(
            CommandExecutionEngine commandProcessor,
            ReplyWriterFactory replyWriterFactory,
            ExecutionIoAdapter<C> ioAdapter,
            ExecutorBacklogBudget backlogBudget,
            ExecutorBackpressureController<C> backpressureController,
            int backpressureLowWatermark,
            long backpressureBytesHighWatermark,
            long backpressureBytesLowWatermark,
            BooleanSupplier running
    ) {
        this.commandProcessor = Objects.requireNonNull(commandProcessor, "commandProcessor");
        this.replyWriterFactory = Objects.requireNonNull(replyWriterFactory, "replyWriterFactory");
        this.ioAdapter = Objects.requireNonNull(ioAdapter, "ioAdapter");
        this.backlogBudget = Objects.requireNonNull(backlogBudget, "backlogBudget");
        this.backpressureController = Objects.requireNonNull(backpressureController, "backpressureController");
        this.backpressureLowWatermark = backpressureLowWatermark;
        this.backpressureBytesHighWatermark = backpressureBytesHighWatermark;
        this.backpressureBytesLowWatermark = backpressureBytesLowWatermark;
        this.running = Objects.requireNonNull(running, "running");
    }

    void execute(CommandExecutorTask<C> task, Collection<C> touchedConnections) {
        if (task == null) {
            return;
        }

        C connection = task.connection;
        if (connection == null) {
            closeRequest(task.request);
            releaseReservedBudget(task.retainedBytes);
            return;
        }

        ExecutionConnectionContext context = connection.context();
        if (context.isClosing()) {
            context.recordSkippedClosing();
            commandsSkippedClosing.increment();
            closeRequest(task.request);
            finishTask(connection, task.retainedBytes, false);
            return;
        }

        boolean executed = false;
        try {
            ReplyWriter writer = replyWriterFactory.newWriter(serverSession(connection), ioAdapter.newReplySink(connection));
            commandProcessor.execute(connection.session(), task.request, writer);
            if (writer.closeAfterReplyRequested()) {
                context.recordCloseAfterReply();
                closeAfterReply.increment();
                connection.markClosing();
            }
            ioAdapter.writeBufferedReply(connection, writer.closeAfterReplyRequested());
            touchedConnections.add(connection);
            commandsExecuted.increment();
            executed = true;
        } catch (Throwable t) {
            executed = true;
            commandsExecuted.increment();
            handleExecutionFailure(connection, context, touchedConnections);
        } finally {
            closeRequest(task.request);
            finishTask(connection, task.retainedBytes, executed);
        }
    }

    void flushPending(Collection<C> touchedConnections) {
        if (touchedConnections == null || touchedConnections.isEmpty()) {
            return;
        }
        ioAdapter.flushPending(touchedConnections);
    }

    void recycleAndRelease(CommandExecutorTask<C> task) {
        if (task == null) {
            return;
        }
        closeRequest(task.request);
        if (task.connection != null) {
            finishTask(task.connection, task.retainedBytes, false);
            return;
        }
        releaseReservedBudget(task.retainedBytes);
    }

    void recoverInputIfPossible(C connection) {
        maybeRecoverInput(connection);
    }

    long commandsExecuted() {
        return commandsExecuted.sum();
    }

    long commandsSkippedClosing() {
        return commandsSkippedClosing.sum();
    }

    long closeAfterReply() {
        return closeAfterReply.sum();
    }

    private void finishTask(C connection, int retainedBytes, boolean executed) {
        try {
            connection.context().recordCommandFinished(retainedBytes, executed);
        } finally {
            releaseReservedBudget(retainedBytes);
        }
        maybeRecoverInput(connection);
    }

    private void releaseReservedBudget(int retainedBytes) {
        backlogBudget.releaseQueuedBytes(retainedBytes);
        backlogBudget.releaseSlot();
    }

    private void maybeRecoverInput(C connection) {
        if (connection == null || !running.getAsBoolean()) {
            return;
        }
        ExecutionConnectionContext context = connection.context();
        boolean pendingOk = context.pending() <= backpressureLowWatermark;
        boolean bytesOk = backpressureBytesHighWatermark <= 0 || context.pendingBytes() <= backpressureBytesLowWatermark;
        boolean globalOk = backlogBudget.isGlobalBackpressureCleared();
        if (!context.isClosing() && pendingOk && bytesOk && globalOk) {
            backpressureController.enableAutoReadIfWeDisabled(connection);
        }
        if (globalOk) {
            backpressureController.scheduleGlobalRecovery();
        }
    }

    private void handleExecutionFailure(
            C connection,
            ExecutionConnectionContext context,
            Collection<C> touchedConnections
    ) {
        try {
            if (connection.markClosing()) {
                backpressureController.disableAutoRead(connection);
            }
        } catch (Throwable ignored) {
            // Ignore best-effort closing bookkeeping after executor-thread failures.
        }

        try {
            ReplyWriter writer = replyWriterFactory.newWriter(serverSession(connection), ioAdapter.newReplySink(connection));
            writer.internalError("ERR internal error");
            writer.requestCloseAfterReply();
            context.recordCloseAfterReply();
            closeAfterReply.increment();
            ioAdapter.writeBufferedReply(connection, true);
            touchedConnections.add(connection);
        } catch (Throwable ignored) {
            // Ignore best-effort internal error reply failures; the connection is already closing.
        }
    }

    private static void closeRequest(yier.bubu.redis.execution.api.ExecutionRequest request) {
        if (request == null) {
            return;
        }
        try {
            request.close();
        } catch (Throwable ignored) {
            // Ignore cleanup failures on executor-owned requests.
        }
    }

    private static ServerSession serverSession(ExecutionConnection connection) {
        var session = connection == null ? null : connection.session();
        return session instanceof ServerSession serverSession ? serverSession : null;
    }
}
