package yier.bubu.redis.app.server;

// Netty server 连接初始化器：显式装配 decode→handle 的 pipeline，并在连接建立时绑定连接态（协议会话与执行器调度状态）。

import io.netty.channel.ChannelInitializer;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.ScheduledFuture;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;
import yier.bubu.redis.execution.api.RedisReplyWriterFactory;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.protocol.resp.netty.RespCommandAdapter;
import yier.bubu.redis.protocol.resp.netty.RespProtocolErrorReplyHandler;
import yier.bubu.redis.protocol.resp.netty.RespRequestDecoder;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

final class YierdisServerChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final YierdisServerRuntimeConfig config;
    private final CommandExecutor<NettyExecutionConnection> executor;
    private final RedisReplyWriterFactory replyWriterFactory;

    YierdisServerChannelInitializer(
            YierdisServerRuntimeConfig config,
            CommandExecutor<NettyExecutionConnection> executor,
            RedisReplyWriterFactory replyWriterFactory
    ) {
        this.config = Objects.requireNonNull(config, "config");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.replyWriterFactory = Objects.requireNonNull(replyWriterFactory, "replyWriterFactory");
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        if (config.clientOutputBufferLimitBytes() > 0) {
            int high = (int) Math.min(Integer.MAX_VALUE, config.clientOutputBufferLimitBytes());
            int low = Math.max(1, high / 2);
            ch.config().setWriteBufferWaterMark(new WriteBufferWaterMark(low, high));
        }

        NettyExecutionConnection.getOrCreate(
                ch,
                config.transactionQueueMaxCommands(),
                config.transactionQueueMaxBytes()
        );

        ch.pipeline().addLast("writeBufferBackpressure", new WriteBufferBackpressureHandler(
                executor,
                config.clientOutputBufferLimitBytes() > 0 ? config.clientOutputBufferOverLimitMillis() : 0
        ));
        if (config.clientIdleTimeoutMillis() > 0) {
            ch.pipeline()
                    .addLast("idleTimeout", new IdleStateHandler(
                            config.clientIdleTimeoutMillis(), 0, 0, TimeUnit.MILLISECONDS
                    ))
                    .addLast("idleTimeoutCloser", new CloseOnReadIdleHandler());
        }
        ch.pipeline()
                .addLast("respRequestDecoder", new RespRequestDecoder(
                        config.protocolMaxBulkBytes(),
                        config.protocolMaxArgs(),
                        config.protocolMaxLineBytes()
                ))
                .addLast("respProtocolErrorReply", new RespProtocolErrorReplyHandler(
                        replyWriterFactory,
                        YierdisServerChannelInitializer::markProtocolErrorClosing
                ))
                .addLast("respCommandAdapter", new RespCommandAdapter())
                .addLast("commandHandler", new YierdisFastCommandHandler(executor, replyWriterFactory));
    }

    private static void markProtocolErrorClosing(io.netty.channel.ChannelHandlerContext ctx) {
        if (ctx == null) {
            return;
        }
        NettyExecutionConnection connection = NettyExecutionConnection.get(ctx.channel());
        if (connection != null && connection.markClosing()) {
            safeSetAutoRead(ctx, false);
        }
    }

    private static void safeSetAutoRead(io.netty.channel.ChannelHandlerContext ctx, boolean enabled) {
        try {
            ctx.channel().config().setAutoRead(enabled);
        } catch (Throwable ignored) {
            // ignore
        }
    }

    /**
     * Disables autoRead when the channel becomes unwritable (outbound buffer high watermark), and asks the
     * executor to re-evaluate autoRead when it becomes writable again.
     */
    private static final class WriteBufferBackpressureHandler extends io.netty.channel.ChannelInboundHandlerAdapter {
        private final CommandExecutor<NettyExecutionConnection> executor;
        private final long outputBufferOverLimitMillis;
        private ScheduledFuture<?> slowClientCloseFuture;

        private WriteBufferBackpressureHandler(
                CommandExecutor<NettyExecutionConnection> executor,
                long outputBufferOverLimitMillis
        ) {
            this.executor = Objects.requireNonNull(executor, "executor");
            this.outputBufferOverLimitMillis = outputBufferOverLimitMillis;
        }

        @Override
        public void channelWritabilityChanged(io.netty.channel.ChannelHandlerContext ctx) throws Exception {
            if (ctx != null) {
                NettyExecutionConnection connection = NettyExecutionConnection.get(ctx.channel());
                if (!ctx.channel().isWritable()) {
                    executor.onTransportUnwritable(connection);
                    scheduleSlowClientClose(ctx);
                } else if (connection != null) {
                    cancelSlowClientClose();
                    executor.onTransportWritable(connection);
                }
            }
            super.channelWritabilityChanged(ctx);
        }

        @Override
        public void handlerRemoved(io.netty.channel.ChannelHandlerContext ctx) throws Exception {
            cancelSlowClientClose();
            super.handlerRemoved(ctx);
        }

        private void scheduleSlowClientClose(io.netty.channel.ChannelHandlerContext ctx) {
            if (outputBufferOverLimitMillis <= 0 || slowClientCloseFuture != null) {
                return;
            }
            slowClientCloseFuture = ctx.executor().schedule(() -> {
                slowClientCloseFuture = null;
                if (!ctx.channel().isWritable()) {
                    ctx.close();
                }
            }, outputBufferOverLimitMillis, TimeUnit.MILLISECONDS);
        }

        private void cancelSlowClientClose() {
            if (slowClientCloseFuture != null) {
                slowClientCloseFuture.cancel(false);
                slowClientCloseFuture = null;
            }
        }
    }

    private static final class CloseOnReadIdleHandler extends io.netty.channel.ChannelInboundHandlerAdapter {
        @Override
        public void userEventTriggered(io.netty.channel.ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent idle && idle.state() == io.netty.handler.timeout.IdleState.READER_IDLE) {
                ctx.close();
                return;
            }
            super.userEventTriggered(ctx, evt);
        }
    }
}
