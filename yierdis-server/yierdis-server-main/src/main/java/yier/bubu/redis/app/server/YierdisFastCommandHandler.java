package yier.bubu.redis.app.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.RedisReplyWriterFactory;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.protocol.resp.netty.InboundReadCreditHandler;
import yier.bubu.redis.protocol.resp.netty.RespProtocolError;

import java.io.IOException;
import java.util.Objects;

public final class YierdisFastCommandHandler extends ChannelInboundHandlerAdapter {
    private static final Logger log = LoggerFactory.getLogger(YierdisFastCommandHandler.class);

    private final CommandExecutor<NettyExecutionConnection> executor;
    private final RedisReplyWriterFactory replyWriterFactory;

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
            CommandExecutor.SubmitRejectReason reject = executor.trySubmit(connection, request, registered.slot());
            if (reject == null) {
                return;
            }
            if (reject == CommandExecutor.SubmitRejectReason.CONNECTION_CLOSING) {
                closeRequest(request);
                registered.slot().cancel(ReplyCleanupOwner.SEQUENCER);
                return;
            }
            try {
                RedisReplyWriter writer = replyWriterFactory.newWriter(connection.session(), registered.slot().sink());
                writer.error("ERR busy " + reject.code());
                registered.slot().markReady(false);
            } catch (Throwable ignored) {
                registered.slot().cancel(ReplyCleanupOwner.SEQUENCER);
            } finally {
                closeRequest(request);
            }
            return;
        }

        if (message instanceof RespProtocolError error) {
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
            if (connection != null && connection.markClosing()) {
                safeDisableAutoRead(ctx);
            }
            ctx.close();
            return;
        }

        log.error("Internal error from {}: {}", remote, logMessage, root);
        if (connection == null || connection.replyGate() == null) {
            ctx.close();
            return;
        }
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
}
