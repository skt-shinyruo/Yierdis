package yier.bubu.redis.app.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yier.bubu.redis.bytes.netty.NettyByteBufSink;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.RedisReplyWriterFactory;
import yier.bubu.redis.execution.executor.CommandExecutor;

import java.util.Objects;

public final class YierdisFastCommandHandler extends SimpleChannelInboundHandler<ExecutionRequest> {
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
    protected void channelRead0(ChannelHandlerContext ctx, ExecutionRequest msg) {
        NettyExecutionConnection connection = requireConnection(ctx);
        CommandExecutor.SubmitRejectReason reject = executor.trySubmit(connection, msg);
        if (reject == null) {
            return;
        }
        if (reject == CommandExecutor.SubmitRejectReason.CONNECTION_CLOSING) {
            msg.close();
            return;
        }

        ByteBuf out = ctx.alloc().buffer();
        try {
            RedisReplyWriter writer = replyWriterFactory.newWriter(connection.session(), new NettyByteBufSink(out));
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
        if (ctx == null) {
            return;
        }

        Throwable root = unwrapDecoderException(cause);
        String logMessage = safeLogMessage(root);
        String remote = String.valueOf(ctx.channel().remoteAddress());
        log.error("Internal error from {}: {}", remote, logMessage, root);

        ByteBuf out = ctx.alloc().buffer();
        try {
            NettyExecutionConnection connection = NettyExecutionConnection.get(ctx.channel());
            if (connection != null && connection.markClosing()) {
                safeDisableAutoRead(ctx);
            }

            RedisReplyWriter writer = newReplyWriter(out, connection);
            writer.internalError("ERR internal error");
            ctx.writeAndFlush(out).addListener(ChannelFutureListener.CLOSE);
            out = null;
        } finally {
            if (out != null) {
                out.release();
            }
        }
    }

    private RedisReplyWriter newReplyWriter(ByteBuf out, NettyExecutionConnection connection) {
        return connection == null
                ? replyWriterFactory.newWriter(new NettyByteBufSink(out))
                : replyWriterFactory.newWriter(connection.session(), new NettyByteBufSink(out));
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
