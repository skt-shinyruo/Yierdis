package yier.bubu.redis.app.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yier.bubu.redis.execution.api.CapacityRegistration;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.RedisReplyWriterFactory;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.execution.executor.ExecutorAdmissionAttempt;
import yier.bubu.redis.protocol.resp.netty.InboundReadCreditHandler;
import yier.bubu.redis.protocol.resp.netty.RespProtocolError;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Objects;

public final class YierdisFastCommandHandler extends ChannelInboundHandlerAdapter {
    private static final Logger log = LoggerFactory.getLogger(YierdisFastCommandHandler.class);
    private static final String DEFERRED_BUSY_ERROR = "ERR busy queue_full";

    private final CommandExecutor<NettyExecutionConnection> executor;
    private final RedisReplyWriterFactory replyWriterFactory;
    private final ArrayDeque<PendingSubmission> pendingSubmissions = new ArrayDeque<>();
    private CapacityRegistration capacityRegistration = CapacityRegistration.NONE;

    public YierdisFastCommandHandler(
            CommandExecutor<NettyExecutionConnection> executor,
            RedisReplyWriterFactory replyWriterFactory
    ) {
        this.executor = Objects.requireNonNull(executor, "executor");
        this.replyWriterFactory = Objects.requireNonNull(replyWriterFactory, "replyWriterFactory");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof RegisteredRespMessage registered) {
            handleRegistered(ctx, registered);
            return;
        }
        if (msg instanceof ExecutionRequest request) {
            closeRequest(request);
            ctx.close();
            return;
        }
        super.channelRead(ctx, msg);
    }

    private void handleRegistered(ChannelHandlerContext ctx, RegisteredRespMessage registered) {
        NettyExecutionConnection connection = requireConnection(ctx);
        Object message;
        try {
            message = registered.takeMessage();
        } catch (Throwable ignored) {
            registered.close();
            return;
        }

        if (message instanceof ExecutionRequest request) {
            PendingSubmission submission = new PendingSubmission(request, registered.slot());
            if (!pendingSubmissions.isEmpty()) {
                pendingSubmissions.addLast(submission);
                pauseExecutorInput(ctx);
                return;
            }
            submitOrDefer(ctx, connection, submission);
            return;
        }

        if (message instanceof RespProtocolError error) {
            cancelCapacityWait();
            completePendingSubmissionsWithError(connection);
            try {
                if (connection.markClosing()) {
                    safeDisableAutoRead(ctx);
                }
                RedisReplyWriter writer = replyWriterFactory.newWriter(connection.session(), registered.slot().sink());
                writer.protocolError(error.message());
                registered.slot().markReady(true);
            } catch (Throwable ignored) {
                registered.slot().cancel(ReplyCleanupOwner.SEQUENCER);
            }
            return;
        }

        registered.slot().cancel(ReplyCleanupOwner.SEQUENCER);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cancelCapacityWait();
        clearPendingSubmissions();
        super.channelInactive(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        cancelCapacityWait();
        clearPendingSubmissions();
        super.handlerRemoved(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (ctx == null) {
            return;
        }

        Throwable root = unwrapDecoderException(cause);
        String logMessage = safeLogMessage(root);
        String remote = String.valueOf(ctx.channel().remoteAddress());

        NettyExecutionConnection connection = NettyExecutionConnection.get(ctx.channel());
        if (root instanceof IOException) {
            log.debug("Transport closed from {}: {}", remote, logMessage);
            cancelCapacityWait();
            clearPendingSubmissions();
            if (connection != null && connection.markClosing()) {
                safeDisableAutoRead(ctx);
            }
            ctx.close();
            return;
        }

        log.error("Internal error from {}: {}", remote, logMessage, root);
        if (connection == null || connection.replyGate() == null) {
            cancelCapacityWait();
            clearPendingSubmissions();
            ctx.close();
            return;
        }
        cancelCapacityWait();
        completePendingSubmissionsWithError(connection);
        if (connection.markClosing()) {
            safeDisableAutoRead(ctx);
        }

        ReplySlot slot = connection.replyGate().tryRegisterTerminalSlot().orElse(null);
        if (slot == null) {
            ctx.close();
            return;
        }
        try {
            RedisReplyWriter writer = replyWriterFactory.newWriter(connection.session(), slot.sink());
            writer.internalError("ERR internal error");
            writer.requestCloseAfterReply();
            slot.markReady(true);
        } catch (Throwable ignored) {
            slot.cancel(ReplyCleanupOwner.SEQUENCER);
            ctx.close();
        }
    }

    private static NettyExecutionConnection requireConnection(ChannelHandlerContext ctx) {
        NettyExecutionConnection connection = NettyExecutionConnection.get(ctx.channel());
        if (connection == null) {
            throw new IllegalStateException("missing NettyExecutionConnection");
        }
        return connection;
    }

    private void submitOrDefer(
            ChannelHandlerContext ctx,
            NettyExecutionConnection connection,
            PendingSubmission submission
    ) {
        ExecutorAdmissionAttempt<NettyExecutionConnection> attempt = executor.tryAcquire(
                connection,
                submission.request.retainedBytes()
        );
        if (attempt instanceof ExecutorAdmissionAttempt.Acquired<NettyExecutionConnection> acquired) {
            acquired.admission().publish(submission.request, submission.slot);
            resumeExecutorInputIfDrained(ctx);
            return;
        }
        if (attempt instanceof ExecutorAdmissionAttempt.Unavailable<NettyExecutionConnection>) {
            pendingSubmissions.addFirst(submission);
            pauseExecutorInput(ctx);
            armCapacityWait(ctx);
            return;
        }
        terminateRejectedSubmission(
                ctx,
                connection,
                submission,
                ((ExecutorAdmissionAttempt.Rejected<NettyExecutionConnection>) attempt).reason()
        );
    }

    private void retryPendingSubmissions(ChannelHandlerContext ctx) {
        capacityRegistration = CapacityRegistration.NONE;
        if (!ctx.channel().isActive()) {
            clearPendingSubmissions();
            return;
        }
        NettyExecutionConnection connection = NettyExecutionConnection.get(ctx.channel());
        if (connection == null) {
            clearPendingSubmissions();
            ctx.close();
            return;
        }
        while (!pendingSubmissions.isEmpty()) {
            PendingSubmission submission = pendingSubmissions.removeFirst();
            ExecutorAdmissionAttempt<NettyExecutionConnection> attempt = executor.tryAcquire(
                    connection,
                    submission.request.retainedBytes()
            );
            if (attempt instanceof ExecutorAdmissionAttempt.Acquired<NettyExecutionConnection> acquired) {
                acquired.admission().publish(submission.request, submission.slot);
                continue;
            }
            if (attempt instanceof ExecutorAdmissionAttempt.Unavailable<NettyExecutionConnection>) {
                pendingSubmissions.addFirst(submission);
                armCapacityWait(ctx);
                return;
            }
            terminateRejectedSubmission(
                    ctx,
                    connection,
                    submission,
                    ((ExecutorAdmissionAttempt.Rejected<NettyExecutionConnection>) attempt).reason()
            );
            if (!ctx.channel().isActive() || connection.context().isClosing()) {
                clearPendingSubmissions();
                return;
            }
        }
        resumeExecutorInputIfDrained(ctx);
    }

    private void armCapacityWait(ChannelHandlerContext ctx) {
        cancelCapacityWait();
        PendingSubmission head = pendingSubmissions.peekFirst();
        if (head == null) {
            resumeExecutorInputIfDrained(ctx);
            return;
        }
        capacityRegistration = executor.onAdmissionAvailable(
                head.request.retainedBytes(),
                () -> ctx.executor().execute(() -> retryPendingSubmissions(ctx))
        );
    }

    private void terminateRejectedSubmission(
            ChannelHandlerContext ctx,
            NettyExecutionConnection connection,
            PendingSubmission submission,
            CommandExecutor.SubmitRejectReason reject
    ) {
        if (reject == CommandExecutor.SubmitRejectReason.REQUEST_TOO_LARGE) {
            try {
                RedisReplyWriter writer = replyWriterFactory.newWriter(connection.session(), submission.slot.sink());
                writer.error("ERR request exceeds executor queue byte limit");
                submission.slot.markReady(false);
            } catch (Throwable ignored) {
                submission.slot.cancel(ReplyCleanupOwner.SEQUENCER);
            } finally {
                closeRequest(submission.request);
            }
            return;
        }
        closeRequest(submission.request);
        submission.slot.cancel(ReplyCleanupOwner.SEQUENCER);
        if (reject == CommandExecutor.SubmitRejectReason.NOT_RUNNING
                && !connection.context().isClosing()) {
            // 正常 shutdown 由 reply sequencer 在 READY/WRITING 回复排空后关闭 transport。
            ctx.close();
        }
    }

    private void cancelCapacityWait() {
        CapacityRegistration registration = capacityRegistration;
        capacityRegistration = CapacityRegistration.NONE;
        registration.cancel();
    }

    private void clearPendingSubmissions() {
        PendingSubmission submission;
        while ((submission = pendingSubmissions.pollFirst()) != null) {
            closeRequest(submission.request);
            submission.slot.cancel(ReplyCleanupOwner.SEQUENCER);
        }
    }

    private void completePendingSubmissionsWithError(NettyExecutionConnection connection) {
        PendingSubmission submission;
        while ((submission = pendingSubmissions.pollFirst()) != null) {
            try {
                RedisReplyWriter writer = replyWriterFactory.newWriter(
                        connection.session(),
                        submission.slot.sink()
                );
                writer.error(DEFERRED_BUSY_ERROR);
                submission.slot.markReady(false);
            } catch (Throwable ignored) {
                submission.slot.cancel(ReplyCleanupOwner.SEQUENCER);
            } finally {
                closeRequest(submission.request);
            }
        }
    }

    private void resumeExecutorInputIfDrained(ChannelHandlerContext ctx) {
        if (!pendingSubmissions.isEmpty()) {
            return;
        }
        InboundReadCreditHandler readCredits = ctx.pipeline().get(InboundReadCreditHandler.class);
        if (readCredits != null) {
            readCredits.resumeExecutorInput();
            return;
        }
        try {
            ctx.channel().config().setAutoRead(true);
        } catch (Throwable ignored) {
        }
    }

    private static void pauseExecutorInput(ChannelHandlerContext ctx) {
        InboundReadCreditHandler readCredits = ctx.pipeline().get(InboundReadCreditHandler.class);
        if (readCredits != null) {
            readCredits.pauseExecutorInput();
            return;
        }
        try {
            ctx.channel().config().setAutoRead(false);
        } catch (Throwable ignored) {
            // ignore
        }
    }

    private static void closeRequest(ExecutionRequest request) {
        if (request == null) {
            return;
        }
        try {
            request.close();
        } catch (Throwable ignored) {
            // 拒绝路径仍需释放 slot，不能因请求清理异常中断。
        }
    }

    private static void safeDisableAutoRead(ChannelHandlerContext ctx) {
        if (ctx == null) {
            return;
        }
        InboundReadCreditHandler readCredits = ctx.pipeline().get(InboundReadCreditHandler.class);
        if (readCredits != null) {
            readCredits.pauseIngress();
            return;
        }
        try {
            ctx.channel().config().setAutoRead(false);
        } catch (Throwable ignored) {
            // ignore
        }
    }

    private static Throwable unwrapDecoderException(Throwable cause) {
        if (cause instanceof DecoderException && cause.getCause() != null) {
            return cause.getCause();
        }
        return cause;
    }

    private static String safeLogMessage(Throwable cause) {
        if (cause == null) {
            return "internal error";
        }
        String msg = cause.getMessage();
        if (msg == null || msg.isBlank()) {
            msg = cause.getClass().getSimpleName();
        }
        // Prevent response splitting via CRLF injection.
        msg = msg.replace('\r', ' ').replace('\n', ' ');
        if (msg.length() > 256) {
            msg = msg.substring(0, 256);
        }
        return msg;
    }

    private record PendingSubmission(ExecutionRequest request, ReplySlot slot) {
        private PendingSubmission {
            Objects.requireNonNull(request, "request");
            Objects.requireNonNull(slot, "slot");
        }
    }
}
