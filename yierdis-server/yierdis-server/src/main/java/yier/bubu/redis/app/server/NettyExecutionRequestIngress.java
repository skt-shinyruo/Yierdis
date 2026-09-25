package yier.bubu.redis.app.server;

import java.util.function.BiFunction;

import lombok.extern.slf4j.Slf4j;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderException;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.execution.executor.ExecutorAdmissionAttempt;
import yier.bubu.redis.protocol.resp.netty.InboundReadCreditHandler;
import yier.bubu.redis.protocol.resp.netty.RespDecodedMessage;
import yier.bubu.redis.protocol.resp.netty.RespProtocolError;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Objects;

@Slf4j
public final class NettyExecutionRequestIngress extends ChannelInboundHandlerAdapter {

    private final CommandExecutor executor;
    private final BiFunction<Integer, BytesSink, RedisReplyWriter> replyWriterFactory;
    private final ArrayDeque<PendingSubmission> pendingSubmissions = new ArrayDeque<>();
    private Runnable capacityRegistration;

    public NettyExecutionRequestIngress(
            CommandExecutor executor,
            BiFunction<Integer, BytesSink, RedisReplyWriter> replyWriterFactory
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
        if (msg instanceof RespDecodedMessage decoded) {
            closeDecoded(decoded);
            // 裸解码消息本不应到达 ingress；关闭同样经 initiateClose 收敛，避免绕过 closing 语义。
            NettyExecutionConnection.initiateClose(ctx.channel());
            return;
        }
        super.channelRead(ctx, msg);
    }

    private void handleRegistered(ChannelHandlerContext ctx, RegisteredRespMessage registered) {
        NettyExecutionConnection connection;
        RespDecodedMessage message;
        try {
            connection = requireConnection(ctx);
            message = registered.takeMessage();
        } catch (Throwable ignored) {
            registered.close();
            return;
        }

        switch (message) {
            case RespDecodedMessage.Request decodedRequest -> {
                PendingSubmission submission = new PendingSubmission(decodedRequest.request(), registered.slot());
                if (!pendingSubmissions.isEmpty()) {
                    pendingSubmissions.addLast(submission);
                    pauseExecutorInput(ctx);
                    return;
                }
                submitOrDefer(ctx, connection, submission);
            }
            case RespProtocolError error -> {
                cancelCapacityWait();
                if (!pendingSubmissions.isEmpty()) {
                    // FIFO：更早的容量延迟提交尚未发布，既不能以 busy 替身回复，也不能在静默取消它们之后
                    // 再刷出这条更晚的终端错误。取消未发布 slot 并随连接拆除收敛（fail-closed EOF）。
                    clearPendingSubmissions();
                    registered.slot().cancel();
                    connection.initiateClose();
                    return;
                }
                try {
                    if (connection.markClosing()) {
                        safeDisableAutoRead(ctx);
                    }
                    RedisReplyWriter writer = controlReplyWriter(connection, registered.slot().sink());
                    writer.controlError(error.message());
                    registered.slot().markReady(true);
                } catch (Throwable ignored) {
                    registered.slot().cancel();
                }
            }
        }
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
        String remote = String.valueOf(ctx.channel().remoteAddress());

        NettyExecutionConnection connection = NettyExecutionConnection.get(ctx.channel());
        if (root instanceof IOException) {
            // 传输层关闭通常是客户端正常断开，堆栈没有价值；这里只留一行原因，因此不把 Throwable 传给日志。
            // 换行必须抹掉：message 由对端可影响，留着它就能在日志里伪造出额外一行；再截断，防对端灌入超长行。
            String reason = String.valueOf(root).replace('\r', ' ').replace('\n', ' ');
            if (reason.length() > 256) {
                reason = reason.substring(0, 256);
            }
            log.debug("Transport closed from {}: {}", remote, reason);
            cancelCapacityWait();
            clearPendingSubmissions();
            if (connection != null && connection.markClosing()) {
                safeDisableAutoRead(ctx);
            }
            NettyExecutionConnection.initiateClose(ctx.channel());
            return;
        }

        log.error("Internal error from {}", remote, root);
        if (connection == null || connection.replyGate() == null) {
            cancelCapacityWait();
            clearPendingSubmissions();
            ctx.close();
            return;
        }
        cancelCapacityWait();
        if (!pendingSubmissions.isEmpty()) {
            // 与协议错误同一 FIFO 契约：取消未发布 slot 时不得再刷出更晚的终端错误。
            clearPendingSubmissions();
            connection.initiateClose();
            return;
        }
        if (connection.markClosing()) {
            safeDisableAutoRead(ctx);
        }

        ReplySlot slot = connection.replyGate().tryRegisterTerminalSlot().orElse(null);
        if (slot == null) {
            connection.initiateClose();
            return;
        }
        try {
            RedisReplyWriter writer = controlReplyWriter(connection, slot.sink());
            writer.controlError("ERR internal error");
            slot.markReady(true);
        } catch (Throwable ignored) {
            slot.cancel();
            connection.initiateClose();
        }
    }

    private static NettyExecutionConnection requireConnection(ChannelHandlerContext ctx) {
        NettyExecutionConnection connection = NettyExecutionConnection.get(ctx.channel());
        if (connection == null) {
            throw new IllegalStateException("missing NettyExecutionConnection");
        }
        return connection;
    }

    // 管线外控制回复（错误行）的编码在两个 RESP 版本下相同，writer 按 session 当前版本构造即可。
    private RedisReplyWriter controlReplyWriter(NettyExecutionConnection connection, BytesSink sink) {
        return replyWriterFactory.apply(connection.session().respVersion(), sink);
    }

    private void submitOrDefer(
            ChannelHandlerContext ctx,
            NettyExecutionConnection connection,
            PendingSubmission submission
    ) {
        SubmissionAttempt result = attemptSubmission(ctx, connection, submission);
        if (result == SubmissionAttempt.PUBLISHED) {
            resumeExecutorInputIfDrained(ctx);
            return;
        }
        if (result == SubmissionAttempt.CAPACITY_UNAVAILABLE) {
            pendingSubmissions.addFirst(submission);
            pauseExecutorInput(ctx);
            armCapacityWait(ctx);
        }
    }

    private SubmissionAttempt attemptSubmission(
            ChannelHandlerContext ctx,
            NettyExecutionConnection connection,
            PendingSubmission submission
    ) {
        ExecutorAdmissionAttempt attempt = executor.tryAcquire(
                connection,
                submission.request.retainedBytes()
        );
        if (attempt instanceof ExecutorAdmissionAttempt.Acquired acquired) {
            acquired.admission().publish(submission.request, submission.slot);
            return SubmissionAttempt.PUBLISHED;
        }
        if (attempt instanceof ExecutorAdmissionAttempt.Unavailable) {
            return SubmissionAttempt.CAPACITY_UNAVAILABLE;
        }
        if (attempt instanceof ExecutorAdmissionAttempt.Rejected rejected) {
            terminateRejectedSubmission(ctx, connection, submission, rejected.reason());
            return SubmissionAttempt.REJECTED;
        }
        throw new IllegalStateException("unsupported executor admission attempt: " + attempt);
    }

    private void retryPendingSubmissions(ChannelHandlerContext ctx) {
        capacityRegistration = null;
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
            SubmissionAttempt result = attemptSubmission(ctx, connection, submission);
            if (result == SubmissionAttempt.PUBLISHED) {
                continue;
            }
            if (result == SubmissionAttempt.CAPACITY_UNAVAILABLE) {
                pendingSubmissions.addFirst(submission);
                armCapacityWait(ctx);
                return;
            }
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
                RedisReplyWriter writer = controlReplyWriter(connection, submission.slot.sink());
                writer.error("ERR request exceeds executor queue byte limit");
                submission.slot.markReady(false);
            } catch (Throwable ignored) {
                submission.slot.cancel();
            } finally {
                closeRequest(submission.request);
            }
            return;
        }
        closeRequest(submission.request);
        submission.slot.cancel();
        if (reject == CommandExecutor.SubmitRejectReason.NOT_RUNNING
                && !connection.context().isClosing()) {
            // 正常 shutdown 由 reply sequencer 在 READY/WRITING 回复排空后关闭 transport。
            ctx.close();
        }
    }

    private void cancelCapacityWait() {
        Runnable registration = capacityRegistration;
        capacityRegistration = null;
        if (registration != null) {
            registration.run();
        }
    }

    private void clearPendingSubmissions() {
        PendingSubmission submission;
        while ((submission = pendingSubmissions.pollFirst()) != null) {
            closeRequest(submission.request);
            submission.slot.cancel();
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

    private static void closeDecoded(RespDecodedMessage decoded) {
        try {
            decoded.close();
        } catch (Throwable ignored) {
            // 非法的裸解码消息仍需继续关闭连接。
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

    private record PendingSubmission(ExecutionRequest request, ReplySlot slot) {
        private PendingSubmission {
            Objects.requireNonNull(request, "request");
            Objects.requireNonNull(slot, "slot");
        }
    }

    private enum SubmissionAttempt {
        PUBLISHED,
        CAPACITY_UNAVAILABLE,
        REJECTED
    }
}
