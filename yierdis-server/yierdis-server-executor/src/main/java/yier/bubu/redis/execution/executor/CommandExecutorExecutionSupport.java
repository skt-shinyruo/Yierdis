package yier.bubu.redis.execution.executor;

import yier.bubu.redis.common.command.ResultUnknownException;
import yier.bubu.redis.execution.api.ExecutionReply;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.RedisReplyWriterFactory;
import yier.bubu.redis.execution.api.ReplyCapacityUnavailableException;
import yier.bubu.redis.execution.api.ReplyTooLargeException;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;

final class CommandExecutorExecutionSupport<C extends ExecutionConnection> {
    private final CommandExecutionEngine commandProcessor;
    private final RedisReplyWriterFactory replyWriterFactory;
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
            RedisReplyWriterFactory replyWriterFactory,
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

    ExecutionAttempt execute(CommandExecutorTask<C> task, Collection<C> touchedConnections) {
        if (task == null) {
            return ExecutionAttempt.CONNECTION_CLOSED;
        }

        if (task.reply != null) {
            return executeWithReply(task);
        }

        C connection = task.connection;
        if (connection == null) {
            closeRequest(task.request);
            releaseReservedBudget(task.retainedBytes);
            return ExecutionAttempt.CONNECTION_CLOSED;
        }

        ExecutionConnectionContext context = connection.context();
        if (context.isClosing()) {
            context.recordSkippedClosing();
            commandsSkippedClosing.increment();
            closeRequest(task.request);
            finishTask(connection, task.retainedBytes, false);
            return ExecutionAttempt.CONNECTION_CLOSED;
        }

        boolean executed = false;
        try {
            RedisReplyWriter writer = replyWriterFactory.newWriter(connection.session(), ioAdapter.newReplySink(connection));
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
        return ExecutionAttempt.COMPLETED;
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
        task.cancelCapacityRegistration();
        closeRequest(task.request);
        cancelReply(task.reply);
        if (task.connection != null) {
            task.connection.context().clearInputPausedByReply();
            finishTask(task.connection, task.retainedBytes, false);
            return;
        }
        releaseReservedBudget(task.retainedBytes);
    }

    void recoverInputIfPossible(C connection) {
        maybeRecoverInput(connection);
    }

    void onConnectionClosed(C connection, Runnable callback) {
        if (connection == null || callback == null) {
            return;
        }
        try {
            ioAdapter.onClose(connection, callback);
        } catch (Throwable ignored) {
            // 连接关闭监听失败时，后续 shutdown 仍会回收队列中的任务。
        }
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

    private ExecutionAttempt executeWithReply(CommandExecutorTask<C> task) {
        C connection = task.connection;
        if (connection == null) {
            closeRequest(task.request);
            cancelReply(task.reply);
            releaseReservedBudget(task.retainedBytes);
            return ExecutionAttempt.CONNECTION_CLOSED;
        }

        ExecutionConnectionContext context = connection.context();
        if (context.isClosing()) {
            context.recordSkippedClosing();
            commandsSkippedClosing.increment();
            closeRequest(task.request);
            cancelReply(task.reply);
            finishTask(connection, task.retainedBytes, false);
            return ExecutionAttempt.CONNECTION_CLOSED;
        }

        boolean terminal = false;
        try {
            RedisReplyWriter writer = replyWriterFactory.newWriter(connection.session(), task.reply.sink());
            commandProcessor.execute(connection.session(), task.request, writer);
            if (writer.closeAfterReplyRequested()) {
                context.recordCloseAfterReply();
                closeAfterReply.increment();
                connection.markClosing();
            }
            task.reply.markReady(writer.closeAfterReplyRequested());
            commandsExecuted.increment();
            terminal = true;
            return ExecutionAttempt.COMPLETED;
        } catch (ReplyCapacityUnavailableException unavailable) {
            if (!task.reply.hasWrittenBytes() && !context.isClosing()) {
                context.markInputPausedByReply();
                backpressureController.disableAutoRead(connection);
                return ExecutionAttempt.REPLY_CAPACITY_BLOCKED;
            }
            terminal = true;
            commandsExecuted.increment();
            handleReplyExecutionFailure(connection, context, task.reply);
            return ExecutionAttempt.CONNECTION_CLOSED;
        } catch (ReplyTooLargeException tooLarge) {
            terminal = true;
            commandsExecuted.increment();
            closeOversizedReply(connection, context, task.reply);
            return ExecutionAttempt.CONNECTION_CLOSED;
        } catch (Throwable failure) {
            terminal = true;
            commandsExecuted.increment();
            if (isResultUnknownFailure(failure)) {
                closeResultUnknown(connection, context, task.reply);
                return ExecutionAttempt.CONNECTION_CLOSED;
            }
            handleReplyExecutionFailure(connection, context, task.reply);
        } finally {
            if (terminal) {
                closeRequest(task.request);
                context.clearInputPausedByReply();
                finishTask(connection, task.retainedBytes, true);
            }
        }
        return ExecutionAttempt.CONNECTION_CLOSED;
    }

    private void releaseReservedBudget(int retainedBytes) {
        backlogBudget.release(retainedBytes);
    }

    private void maybeRecoverInput(C connection) {
        if (connection == null || !running.getAsBoolean()) {
            return;
        }
        ExecutionConnectionContext context = connection.context();
        boolean pendingOk = context.pending() <= backpressureLowWatermark;
        boolean bytesOk = backpressureBytesHighWatermark <= 0 || context.pendingBytes() <= backpressureBytesLowWatermark;
        boolean globalOk = backlogBudget.isGlobalBackpressureCleared();
        if (!context.isClosing() && !context.inputPausedByReply() && pendingOk && bytesOk && globalOk) {
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
            RedisReplyWriter writer = replyWriterFactory.newWriter(connection.session(), ioAdapter.newReplySink(connection));
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

    private void handleReplyExecutionFailure(
            C connection,
            ExecutionConnectionContext context,
            ExecutionReply reply
    ) {
        try {
            if (connection.markClosing()) {
                backpressureController.disableAutoRead(connection);
            }
        } catch (Throwable ignored) {
            // 连接关闭记录失败时仍必须归还 reply 所有权。
        }

        if (reply.hasWrittenBytes()) {
            cancelReply(reply);
            closeTransport(connection);
            return;
        }
        try {
            RedisReplyWriter writer = replyWriterFactory.newWriter(connection.session(), reply.sink());
            writer.internalError("ERR internal error");
            writer.requestCloseAfterReply();
            context.recordCloseAfterReply();
            closeAfterReply.increment();
            reply.markReady(true);
        } catch (Throwable ignored) {
            cancelReply(reply);
            closeTransport(connection);
        }
    }

    private void closeResultUnknown(
            C connection,
            ExecutionConnectionContext context,
            ExecutionReply reply
    ) {
        try {
            if (connection.markClosing()) {
                backpressureController.disableAutoRead(connection);
            }
        } catch (Throwable ignored) {
            // 结果可见性未知时，关闭 transport 仍优先于 closing 指标更新。
        }
        reply.markResultUnknown();
        cancelReply(reply);
        closeTransport(connection);
    }

    private void closeOversizedReply(
            C connection,
            ExecutionConnectionContext context,
            ExecutionReply reply
    ) {
        try {
            if (connection.markClosing()) {
                backpressureController.disableAutoRead(connection);
            }
        } catch (Throwable ignored) {
        }
        // 超限回复不能复用同一槽位补发内部错误，否则客户端会把不可完整交付的结果误判为确定失败。
        cancelReply(reply);
        closeTransport(connection);
    }

    private void closeTransport(C connection) {
        try {
            ioAdapter.closeConnection(connection);
        } catch (Throwable ignored) {
            // transport close is best-effort after ownership was already canceled.
        }
    }

    private static boolean isResultUnknownFailure(Throwable failure) {
        for (Throwable current = failure; current != null && current.getCause() != current; current = current.getCause()) {
            if (current instanceof ResultUnknownException) {
                return true;
            }
        }
        return false;
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

    private static void cancelReply(ExecutionReply reply) {
        if (reply == null) {
            return;
        }
        try {
            reply.cancel();
        } catch (Throwable ignored) {
            // 请求回收不能被 transport cleanup 异常打断。
        }
    }

}
