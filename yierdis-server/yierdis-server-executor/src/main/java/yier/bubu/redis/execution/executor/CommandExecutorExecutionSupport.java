package yier.bubu.redis.execution.executor;

import yier.bubu.redis.common.command.ResultUnknownException;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.ExecutionReply;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.RedisReplyRenderer;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.RedisReplyWriterFactory;
import yier.bubu.redis.execution.api.ReplyReservationResult;
import yier.bubu.redis.execution.api.ReplySizer;
import yier.bubu.redis.execution.api.ValidationResult;

import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;

final class CommandExecutorExecutionSupport<C extends ExecutionConnection> {
    private final CommandExecutionEngine commandProcessor;
    private final ReplySizer replySizer;
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
            ReplySizer replySizer,
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
        this.replySizer = Objects.requireNonNull(replySizer, "replySizer");
        this.replyWriterFactory = Objects.requireNonNull(replyWriterFactory, "replyWriterFactory");
        this.ioAdapter = Objects.requireNonNull(ioAdapter, "ioAdapter");
        this.backlogBudget = Objects.requireNonNull(backlogBudget, "backlogBudget");
        this.backpressureController = Objects.requireNonNull(backpressureController, "backpressureController");
        this.backpressureLowWatermark = backpressureLowWatermark;
        this.backpressureBytesHighWatermark = backpressureBytesHighWatermark;
        this.backpressureBytesLowWatermark = backpressureBytesLowWatermark;
        this.running = Objects.requireNonNull(running, "running");
    }

    ExecutionAttempt execute(CommandExecutorTask<C> task) {
        C connection = task.connection;
        ExecutionConnectionContext context = connection.context();
        if (context.isClosing()) {
            context.recordSkippedClosing();
            commandsSkippedClosing.increment();
            task.cancelCapacityRegistration();
            closePrepared(task);
            closeRequest(task.request);
            cancelReply(task.reply);
            finishTask(connection, task.retainedBytes, false);
            return ExecutionAttempt.CONNECTION_CLOSED;
        }

        boolean terminal = false;
        boolean executed = false;
        try {
            if (task.prepared == null) {
                task.prepared = Objects.requireNonNull(
                        commandProcessor.prepare(connection.session(), task.request),
                        "command engine returned null prepared command"
                );
                task.replyPlan = Objects.requireNonNull(
                        replySizer.plan(connection.session(), task.prepared.reservationShape()),
                        "reply sizer returned null plan"
                );
            }

            ReplyReservationResult reservation = task.reply.tryReserve(task.replyPlan);
            if (reservation == ReplyReservationResult.WAITING) {
                context.markInputPausedByReply();
                backpressureController.disableAutoRead(connection);
                return ExecutionAttempt.REPLY_CAPACITY_BLOCKED;
            }
            if (reservation == ReplyReservationResult.CLOSED) {
                terminal = true;
                return ExecutionAttempt.CONNECTION_CLOSED;
            }
            if (reservation == ReplyReservationResult.TOO_LARGE) {
                closeOversizedReply(connection, task.reply);
                terminal = true;
                return ExecutionAttempt.CONNECTION_CLOSED;
            }

            task.cancelCapacityRegistration();

            if (task.prepared.validateBeforeExecute() == ValidationResult.STALE) {
                task.closePrepared();
                return ExecutionAttempt.REPREPARE;
            }

            CommandExecutionContext execution = CommandExecutionContext.forSession(connection.session());
            CommandResult result = Objects.requireNonNull(
                    task.prepared.execute(execution),
                    "prepared command returned null result"
            );
            executed = true;
            RedisReplyWriter writer = replyWriterFactory.newWriter(
                    connection.session(), task.reply.sink());
            RedisReplyRenderer.render(result.reply(), writer);
            if (result.closeAfterReply()) {
                context.recordCloseAfterReply();
                closeAfterReply.increment();
                connection.markClosing();
            }
            task.reply.markReady(result.closeAfterReply());
            commandsExecuted.increment();
            terminal = true;
            return ExecutionAttempt.COMPLETED;
        } catch (Throwable failure) {
            terminal = true;
            if (executed) {
                commandsExecuted.increment();
            }
            if (executed || isResultUnknownFailure(failure)) {
                closeResultUnknown(connection, task.reply, failure);
                return ExecutionAttempt.CONNECTION_CLOSED;
            }
            handleReplyExecutionFailure(connection, context, task.reply);
        } finally {
            if (terminal) {
                task.cancelCapacityRegistration();
                closePrepared(task);
                closeRequest(task.request);
                context.clearInputPausedByReply();
                finishTask(connection, task.retainedBytes, executed);
            }
        }
        return ExecutionAttempt.CONNECTION_CLOSED;
    }

    void recycleAndRelease(CommandExecutorTask<C> task) {
        try {
            task.cancelCapacityRegistration();
        } catch (Throwable ignored) {
            // reply 提供的容量监听取消失败时，request、reply 与 backlog 所有权仍必须继续归还。
        }
        closePrepared(task);
        closeRequest(task.request);
        cancelReply(task.reply);
        task.connection.context().clearInputPausedByReply();
        finishTask(task.connection, task.retainedBytes, false);
    }

    void recoverInputIfPossible(C connection) {
        maybeRecoverInput(connection);
    }

    void onConnectionClosed(C connection, Runnable callback) {
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

    private static void closePrepared(CommandExecutorTask<?> task) {
        try {
            task.closePrepared();
        } catch (Throwable ignored) {
            // Terminal cleanup must continue through request, reply, and backlog ownership.
        }
    }

    private void releaseReservedBudget(int retainedBytes) {
        backlogBudget.release(retainedBytes);
    }

    private void maybeRecoverInput(C connection) {
        if (!running.getAsBoolean()) {
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
            ExecutionReply reply,
            Throwable primaryFailure
    ) {
        runCleanupSuppressing(primaryFailure, () -> {
            if (connection.markClosing()) {
                backpressureController.disableAutoRead(connection);
            }
        });
        runCleanupSuppressing(primaryFailure, reply::markResultUnknown);
        runCleanupSuppressing(primaryFailure, reply::cancel);
        runCleanupSuppressing(primaryFailure, () -> ioAdapter.closeConnection(connection));
    }

    private static void runCleanupSuppressing(Throwable primaryFailure, Runnable cleanup) {
        try {
            cleanup.run();
        } catch (Throwable cleanupFailure) {
            if (cleanupFailure != primaryFailure) {
                primaryFailure.addSuppressed(cleanupFailure);
            }
        }
    }

    private void closeOversizedReply(
            C connection,
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
        try {
            request.close();
        } catch (Throwable ignored) {
            // Ignore cleanup failures on executor-owned requests.
        }
    }

    private static void cancelReply(ExecutionReply reply) {
        try {
            reply.cancel();
        } catch (Throwable ignored) {
            // 请求回收不能被 transport cleanup 异常打断。
        }
    }

}
