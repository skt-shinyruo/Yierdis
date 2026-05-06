package yier.bubu.redis.app.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yier.bubu.redis.bytes.netty.NettyByteBufSink;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.contract.ReplyWriterFactory;
import yier.bubu.redis.execution.executor.CommandExecutor;

import java.util.Objects;

public final class YierdisFastCommandHandler extends SimpleChannelInboundHandler<ExecutionRequest> {
    private static final Logger log = LoggerFactory.getLogger(YierdisFastCommandHandler.class);

    private final CommandExecutor<NettyExecutionConnection> executor;
    private final ReplyWriterFactory replyWriterFactory;

    public YierdisFastCommandHandler(
            CommandExecutor<NettyExecutionConnection> executor,
            ReplyWriterFactory replyWriterFactory
    ) {
        this.executor = Objects.requireNonNull(executor, "executor");
        this.replyWriterFactory = Objects.requireNonNull(replyWriterFactory, "replyWriterFactory");
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ExecutionRequest msg) {
        NettyExecutionConnection connection = requireConnection(ctx);
        CommandExecutor.SubmitRejectReason reject = executor.trySubmit(connection, msg);
        if (reject == null) {
            return;
        }

        ByteBuf out = ctx.alloc().buffer();
        try {
            ReplyWriter writer = replyWriterFactory.newWriter(new NettyByteBufSink(out));
            writer.error("ERR busy " + reject.code());
            ctx.writeAndFlush(out);
            out = null;
        } finally {
            msg.close();
            if (out != null) {
                out.release();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Best-effort: return an error reply.
        // Protocol errors should keep the connection alive (decoder will resync when possible).
        // Internal errors may close the connection to avoid side effects from already-queued commands.
        if (ctx == null) {
            return;
        }

        Throwable root = unwrapDecoderException(cause);
        String rawMessage = root == null ? null : root.getMessage();
        String logMessage = safeLogMessage(root);
        String remote = String.valueOf(ctx.channel().remoteAddress());
        boolean protocolError = rawMessage != null && rawMessage.startsWith("Protocol error");

        if (protocolError) {
            // Protocol errors are often client-driven; keep logs low-noise by default.
            log.debug("Protocol error from {}: {}", remote, logMessage);
        } else {
            log.error("Internal error from {}: {}", remote, logMessage, root);
        }

        ByteBuf out = ctx.alloc().buffer();
        try {
            ReplyWriter writer = newReplyWriter(out, ctx);
            if (protocolError) {
                // 回包的 message 净化/限长由协议层 writer SSOT 统一处理，handler 不做重复净化避免漂移。
                writer.protocolError(rawMessage);
            } else {
                // 标记该连接进入 closing：避免 internal error 触发 close 后，已入队命令仍在 executor 中继续执行产生副作用。
                NettyExecutionConnection connection = NettyExecutionConnection.get(ctx.channel());
                if (connection != null && connection.markClosing()) {
                    safeDisableAutoRead(ctx);
                }
                writer.internalError("ERR internal error");
            }
            if (protocolError) {
                ctx.writeAndFlush(out);
            } else {
                ctx.writeAndFlush(out).addListener(ChannelFutureListener.CLOSE);
            }
            out = null;
        } finally {
            if (out != null) {
                out.release();
            }
        }
    }

    private ReplyWriter newReplyWriter(ByteBuf out, ChannelHandlerContext ctx) {
        return replyWriterFactory.newWriter(new NettyByteBufSink(out));
    }

    private static NettyExecutionConnection requireConnection(ChannelHandlerContext ctx) {
        NettyExecutionConnection connection = NettyExecutionConnection.get(ctx.channel());
        if (connection == null) {
            throw new IllegalStateException("missing NettyExecutionConnection");
        }
        return connection;
    }

    private static void safeDisableAutoRead(ChannelHandlerContext ctx) {
        if (ctx == null) {
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
