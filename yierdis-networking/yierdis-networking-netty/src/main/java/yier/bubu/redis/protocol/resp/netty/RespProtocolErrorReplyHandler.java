package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import yier.bubu.redis.bytes.netty.NettyByteBufSink;
import yier.bubu.redis.execution.api.ReplyWriter;
import yier.bubu.redis.execution.api.ReplyWriterFactory;

import java.util.Objects;
import java.util.function.Consumer;

public final class RespProtocolErrorReplyHandler extends ChannelInboundHandlerAdapter {
    private final ReplyWriterFactory replyWriterFactory;
    private final Consumer<ChannelHandlerContext> closeAfterReplyObserver;
    private boolean closing;

    public RespProtocolErrorReplyHandler(ReplyWriterFactory replyWriterFactory) {
        this(replyWriterFactory, ctx -> {});
    }

    public RespProtocolErrorReplyHandler(
            ReplyWriterFactory replyWriterFactory,
            Consumer<ChannelHandlerContext> closeAfterReplyObserver
    ) {
        this.replyWriterFactory = Objects.requireNonNull(replyWriterFactory, "replyWriterFactory");
        this.closeAfterReplyObserver = closeAfterReplyObserver == null ? ctx -> {} : closeAfterReplyObserver;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (closing) {
            closeIfPossible(msg);
            return;
        }
        if (!(msg instanceof RespProtocolError error)) {
            super.channelRead(ctx, msg);
            return;
        }
        if (error.closeAfterReply()) {
            closing = true;
            safeDisableAutoRead(ctx);
            closeAfterReplyObserver.accept(ctx);
        }
        ByteBuf out = ctx.alloc().buffer();
        try {
            ReplyWriter writer = replyWriterFactory.newWriter(new NettyByteBufSink(out));
            writer.protocolError(error.message());
            if (error.closeAfterReply()) {
                ctx.writeAndFlush(out).addListener(ChannelFutureListener.CLOSE);
            } else {
                ctx.writeAndFlush(out);
            }
            out = null;
        } finally {
            if (out != null) {
                out.release();
            }
        }
    }

    private static void closeIfPossible(Object msg) {
        if (msg instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // ignore
            }
            return;
        }
        ReferenceCountUtil.release(msg);
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
}
