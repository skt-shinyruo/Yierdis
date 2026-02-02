package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yier.bubu.redis.bytes.netty.NettyByteBufSink;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespWriter;

import java.util.Objects;

public final class YierdisFastCommandHandler extends SimpleChannelInboundHandler<RespCommand> {
    private static final Logger log = LoggerFactory.getLogger(YierdisFastCommandHandler.class);

    private final NettyCommandExecutor nettyExecutor;

    public YierdisFastCommandHandler(NettyCommandExecutor executor) {
        this.nettyExecutor = Objects.requireNonNull(executor, "executor");
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RespCommand msg) {
        boolean accepted = nettyExecutor.trySubmit(ctx, msg);
        if (accepted) {
            // 执行器接管 msg 的生命周期，负责 recycle。
            return;
        }

        // 队列满或服务关闭：返回 busy 错误并回收命令帧，避免积压导致 OOM。
        ByteBuf out = ctx.alloc().buffer();
        try {
            new RespWriter(new NettyByteBufSink(out), ServerConnectionState.getOrCreate(ctx.channel())).error("ERR busy");
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
        // Best-effort: return a RESP error and close the connection.
        // This covers protocol decode errors (e.g. invalid RESP frames) and unexpected runtime errors.
        if (ctx == null) {
            return;
        }
        // 标记该连接进入 closing：避免 protocol/internal error 触发 close 后，已入队命令仍在 executor 中继续执行产生副作用。
        ServerConnectionState conn = ServerConnectionState.getOrCreate(ctx.channel());
        conn.markClosing();
        nettyExecutor.disableAutoRead(ctx.channel());

        Throwable root = unwrapDecoderException(cause);
        String message = safeErrorMessage(root);
        String remote = String.valueOf(ctx.channel().remoteAddress());
        String err = message.startsWith("Protocol error")
                ? "ERR " + message
                : "ERR internal error";

        if (message.startsWith("Protocol error")) {
            // Protocol errors are often client-driven; keep logs low-noise by default.
            log.debug("Protocol error from {}: {}", remote, message);
        } else {
            log.error("Internal error from {}: {}", remote, message, root);
        }

        ByteBuf out = ctx.alloc().buffer();
        try {
            new RespWriter(new NettyByteBufSink(out), ServerConnectionState.getOrCreate(ctx.channel())).error(err);
            ctx.writeAndFlush(out).addListener(ChannelFutureListener.CLOSE);
            out = null;
        } finally {
            if (out != null) {
                out.release();
            }
        }
    }

    private static Throwable unwrapDecoderException(Throwable cause) {
        if (cause instanceof DecoderException && cause.getCause() != null) {
            return cause.getCause();
        }
        return cause;
    }

    private static String safeErrorMessage(Throwable cause) {
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
