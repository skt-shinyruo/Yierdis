package yier.bubu.redis.execution.executor;

import java.util.function.BiFunction;

import lombok.extern.slf4j.Slf4j;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.common.command.ResultUnknownException;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.ExecutionReply;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.RedisReplyRenderer;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyReservationResult;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ValidationResult;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;

@Slf4j
final class CommandExecutorExecutionSupport {

    private final BiFunction<CommandSession, ExecutionRequest, PreparedCommand> commandProcessor;
    private final BiFunction<Integer, ReplyShape, ReplyPlan> replySizer;
    private final BiFunction<Integer, BytesSink, RedisReplyWriter> replyWriterFactory;
    private final ExecutionIoAdapter ioAdapter;
    private final ExecutorBacklogBudget backlogBudget;
    private final ExecutorBackpressureController backpressureController;
    private final int backpressureLowWatermark;
    private final long backpressureBytesHighWatermark;
    private final long backpressureBytesLowWatermark;
    private final BooleanSupplier running;
    private final LongAdder commandsExecuted = new LongAdder();
    private final LongAdder commandsSkippedClosing = new LongAdder();
    private final LongAdder closeAfterReply = new LongAdder();

    CommandExecutorExecutionSupport(
            BiFunction<CommandSession, ExecutionRequest, PreparedCommand> commandProcessor,
            BiFunction<Integer, ReplyShape, ReplyPlan> replySizer,
            BiFunction<Integer, BytesSink, RedisReplyWriter> replyWriterFactory,
            ExecutionIoAdapter ioAdapter,
            ExecutorBacklogBudget backlogBudget,
            ExecutorBackpressureController backpressureController,
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

    ExecutionAttempt execute(CommandExecutorTask task) {
        ExecutionConnection connection = task.connection;
        ExecutionConnectionContext context = connection.context();
        // 除 closing 标记外还回看 transport 活性：某条关闭路径漏掉 markClosing 时，
        // 已入队命令也不允许在断开的连接上继续产生 side effect。
        if (context.isClosing() || !ioAdapter.isActive(connection)) {
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
                        commandProcessor.apply(connection.session(), task.request),
                        "command engine returned null prepared command"
                );
                // 协议版本在 prepare/预留时刻读取一次并捕获进 reply plan，渲染使用同一份捕获值；
                // execute 期才切换版本的命令（HELLO）已在 prepare 时声明协商后版本。
                int replyProtocolVersion = task.prepared.replyProtocolVersion()
                        .orElseGet(() -> connection.session().respVersion());
                task.replyPlan = Objects.requireNonNull(
                        replySizer.apply(replyProtocolVersion, task.prepared.reservationShape()),
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

            CommandSession execution = connection.session();
            CommandResult result = Objects.requireNonNull(
                    task.prepared.execute(execution),
                    "prepared command returned null result"
            );
            executed = true;
            RedisReplyWriter writer = replyWriterFactory.apply(
                    task.replyPlan.protocolVersion(), task.reply.sink());
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
            // 命令失败时客户端只会看到 "ERR internal error"，不记录的话 handler 的 bug 对运维完全不可见。
            handleReplyExecutionFailure(connection, context, task.reply, argvOf(task.request), failure);
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

    void recycleAndRelease(CommandExecutorTask task) {
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

    void recoverInputIfPossible(ExecutionConnection connection) {
        maybeRecoverInput(connection);
    }

    void onConnectionClosed(ExecutionConnection connection, Runnable callback) {
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

    private void finishTask(ExecutionConnection connection, int retainedBytes, boolean executed) {
        try {
            connection.context().recordCommandFinished(retainedBytes, executed);
        } finally {
            releaseReservedBudget(retainedBytes);
        }
        maybeRecoverInput(connection);
    }

    private static void closePrepared(CommandExecutorTask task) {
        try {
            task.closePrepared();
        } catch (Throwable ignored) {
            // Terminal cleanup must continue through request, reply, and backlog ownership.
        }
    }

    private void releaseReservedBudget(int retainedBytes) {
        backlogBudget.release(retainedBytes);
    }

    private void maybeRecoverInput(ExecutionConnection connection) {
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
            ExecutionConnection connection,
            ExecutionConnectionContext context,
            ExecutionReply reply,
            String commandLine,
            Throwable failure
    ) {
        // 走到这里说明命令在产出结果前就失败了：客户端只会收到 "ERR internal error"，
        // 这条带堆栈的日志是运维能拿到的唯一线索。完整 argv（含参数与 value）原样进日志，
        // 不截断、不做换行消毒。
        log.error("command {} failed before producing a result", commandLine, failure);

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
            // 控制错误编码在两个 RESP 版本下相同，按 session 当前版本创建 writer 即可。
            RedisReplyWriter writer = replyWriterFactory.apply(connection.session().respVersion(), reply.sink());
            writer.controlError("ERR internal error");
            context.recordCloseAfterReply();
            closeAfterReply.increment();
            reply.markReady(true);
        } catch (Throwable ignored) {
            cancelReply(reply);
            closeTransport(connection);
        }
    }

    /**
     * 尽力取出完整 argv 拼成一行，只用于日志。
     * <p>
     * argv 是客户端发来的原始字节：不截断、不做换行消毒、含全部参数与 value，原样解码进日志。
     * 任何取值失败都退化成占位符：记录失败原因的过程绝不能反过来把原始失败盖掉。
     */
    private static String argvOf(ExecutionRequest request) {
        try {
            if (request == null || request.argc() < 1) {
                return "<unknown>";
            }
            StringBuilder argv = new StringBuilder();
            for (int i = 0; i < request.argc(); i++) {
                if (i > 0) {
                    argv.append(' ');
                }
                byte[] arg = request.isNull(i) ? null : request.readOnlyByteArray(i);
                argv.append(arg == null || arg.length == 0 ? "<null>" : new String(arg, StandardCharsets.UTF_8));
            }
            return argv.toString();
        } catch (Throwable argvExtractionFailure) {
            return "<unknown>";
        }
    }

    private void closeResultUnknown(
            ExecutionConnection connection,
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
            ExecutionConnection connection,
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

    private void closeTransport(ExecutionConnection connection) {
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
