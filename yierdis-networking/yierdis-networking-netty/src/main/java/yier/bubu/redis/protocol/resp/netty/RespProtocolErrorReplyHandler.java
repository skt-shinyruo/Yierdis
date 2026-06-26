package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import yier.bubu.redis.bytes.netty.NettyByteBufSink;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.RedisReplyWriterFactory;
import yier.bubu.redis.execution.api.Session;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public final class RespProtocolErrorReplyHandler extends ChannelInboundHandlerAdapter {
    private final RedisReplyWriterFactory replyWriterFactory;
    private final Function<ChannelHandlerContext, Session> sessionProvider;
    private final Predicate<ChannelHandlerContext> closingStateProvider;
    private final Consumer<ChannelHandlerContext> closeAfterReplyObserver;
    private final Consumer<Object> droppedMessageCloser;
    private volatile boolean closingStarted;

    public RespProtocolErrorReplyHandler(RedisReplyWriterFactory replyWriterFactory) {
        this(replyWriterFactory, ctx -> null, ctx -> false, ctx -> {});
    }

    public RespProtocolErrorReplyHandler(
            RedisReplyWriterFactory replyWriterFactory,
            Consumer<ChannelHandlerContext> closeAfterReplyObserver
    ) {
        this(replyWriterFactory, ctx -> null, ctx -> false, closeAfterReplyObserver);
    }

    public RespProtocolErrorReplyHandler(
            RedisReplyWriterFactory replyWriterFactory,
            Function<ChannelHandlerContext, Session> sessionProvider,
            Predicate<ChannelHandlerContext> closingStateProvider,
            Consumer<ChannelHandlerContext> closeAfterReplyObserver
    ) {
        this(
                replyWriterFactory,
                sessionProvider,
                closingStateProvider,
                closeAfterReplyObserver,
                RespProtocolErrorReplyHandler::closeIfPossible
        );
    }

    RespProtocolErrorReplyHandler(
            RedisReplyWriterFactory replyWriterFactory,
            Function<ChannelHandlerContext, Session> sessionProvider,
            Predicate<ChannelHandlerContext> closingStateProvider,
            Consumer<ChannelHandlerContext> closeAfterReplyObserver,
            Consumer<Object> droppedMessageCloser
    ) {
        this.replyWriterFactory = Objects.requireNonNull(replyWriterFactory, "replyWriterFactory");
        this.sessionProvider = sessionProvider == null ? ctx -> null : sessionProvider;
        this.closingStateProvider = closingStateProvider == null ? ctx -> false : closingStateProvider;
        this.closeAfterReplyObserver = closeAfterReplyObserver == null ? ctx -> {} : closeAfterReplyObserver;
        this.droppedMessageCloser = droppedMessageCloser == null
                ? RespProtocolErrorReplyHandler::closeIfPossible
                : droppedMessageCloser;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (closingStarted || closingStateProvider.test(ctx)) {
            droppedMessageCloser.accept(msg);
            return;
        }
        if (!(msg instanceof RespProtocolError error)) {
            super.channelRead(ctx, msg);
            return;
        }
        if (error.closeAfterReply()) {
            closingStarted = true;
            safeDisableAutoRead(ctx);
            closeAfterReplyObserver.accept(ctx);
        }
        ByteBuf out = ctx.alloc().buffer();
        try {
            Session session = sessionProvider.apply(ctx);
            RedisReplyWriter writer = session == null
                    ? replyWriterFactory.newWriter(new NettyByteBufSink(out))
                    : replyWriterFactory.newWriter(session, new NettyByteBufSink(out));
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
