package yier.bubu.redis.app.server;

import java.util.function.BiFunction;

// Netty server 连接初始化器：显式装配 decode→handle 的 pipeline，并在连接建立时绑定连接态（协议会话与执行器调度状态）。

import io.netty.channel.ChannelInitializer;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.protocol.resp.netty.InboundByteAccountingHandler;
import yier.bubu.redis.protocol.resp.netty.InboundConnectionMemory;
import yier.bubu.redis.protocol.resp.netty.InboundMemoryBudget;
import yier.bubu.redis.protocol.resp.netty.InboundReadCreditHandler;
import yier.bubu.redis.protocol.resp.netty.RespRequestDecoder;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

final class YierdisServerChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final YierdisServerRuntimeConfig config;
    private final CommandExecutor<NettyExecutionConnection> executor;
    private final BiFunction<CommandSession, BytesSink, RedisReplyWriter> replyWriterFactory;
    private final InboundMemoryBudget inboundMemoryBudget;
    private final OutboundMemoryBudget outboundMemoryBudget;
    private final ChildChannelRegistry childChannelRegistry;
    private final ReplyEgressStats replyEgressStats;

    YierdisServerChannelInitializer(
            YierdisServerRuntimeConfig config,
            CommandExecutor<NettyExecutionConnection> executor,
            BiFunction<CommandSession, BytesSink, RedisReplyWriter> replyWriterFactory
    ) {
        this(
                config,
                executor,
                replyWriterFactory,
                new InboundMemoryBudget(config.protocolGlobalInFlightBytes()),
                new OutboundMemoryBudget(config.replyGlobalCapacityBytes()),
                new ChildChannelRegistry(config.maxClients()),
                new ReplyEgressStats()
        );
    }

    YierdisServerChannelInitializer(
            YierdisServerRuntimeConfig config,
            CommandExecutor<NettyExecutionConnection> executor,
            BiFunction<CommandSession, BytesSink, RedisReplyWriter> replyWriterFactory,
            InboundMemoryBudget inboundMemoryBudget,
            OutboundMemoryBudget outboundMemoryBudget,
            ChildChannelRegistry childChannelRegistry
    ) {
        this(
                config,
                executor,
                replyWriterFactory,
                inboundMemoryBudget,
                outboundMemoryBudget,
                childChannelRegistry,
                new ReplyEgressStats()
        );
    }

    YierdisServerChannelInitializer(
            YierdisServerRuntimeConfig config,
            CommandExecutor<NettyExecutionConnection> executor,
            BiFunction<CommandSession, BytesSink, RedisReplyWriter> replyWriterFactory,
            InboundMemoryBudget inboundMemoryBudget,
            OutboundMemoryBudget outboundMemoryBudget,
            ChildChannelRegistry childChannelRegistry,
            ReplyEgressStats replyEgressStats
    ) {
        this.config = Objects.requireNonNull(config, "config");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.replyWriterFactory = Objects.requireNonNull(replyWriterFactory, "replyWriterFactory");
        this.inboundMemoryBudget = Objects.requireNonNull(inboundMemoryBudget, "inboundMemoryBudget");
        this.outboundMemoryBudget = Objects.requireNonNull(outboundMemoryBudget, "outboundMemoryBudget");
        this.childChannelRegistry = Objects.requireNonNull(childChannelRegistry, "childChannelRegistry");
        this.replyEgressStats = Objects.requireNonNull(replyEgressStats, "replyEgressStats");
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        if (childChannelRegistry.admit(ch) != ChildChannelRegistry.AdmissionResult.ACCEPTED) {
            return;
        }
        boolean lifecycleBound = false;
        try {
            if (config.clientOutputBufferLimitBytes() > 0) {
                int high = (int) Math.min(Integer.MAX_VALUE, config.clientOutputBufferLimitBytes());
                int low = Math.max(1, high / 2);
                ch.config().setWriteBufferWaterMark(new WriteBufferWaterMark(low, high));
            }

            NettyExecutionConnection executionConnection = NettyExecutionConnection.getOrCreate(
                    ch,
                    config.transactionQueueMaxCommands(),
                    config.transactionQueueMaxBytes()
            );
            executionConnection.bindOwnerTaskExecutor(task -> executor.executeOwnerTask(task));
            ch.closeFuture().addListener(ignored -> executionConnection.markClosing());

            InboundConnectionMemory inboundConnection = new InboundConnectionMemory(
                    perConnectionHardLimit(config),
                    ImmediateEventExecutor.INSTANCE,
                    () -> { }
            );
            InboundReadCreditHandler inboundReadCredit = new InboundReadCreditHandler(
                    inboundMemoryBudget,
                    inboundConnection,
                    receiveBufferCapacity(config)
            );
            OutboundConnectionMemory outboundConnection = outboundMemoryBudget.openConnection(
                    config.replyPerConnectionCapacityBytes()
            );
            ConnectionReplySequencer replySequencer = new ConnectionReplySequencer(
                    ch,
                    outboundConnection,
                    inboundReadCredit::pauseIngress,
                    slot -> BoundedChunkedReplySink.forChannel(
                            slot,
                            ch,
                            config.replyChunkPayloadBytes(),
                            config.replyControlReservationBytes(),
                            config.replyMaxTotalBytes()
                    ),
                    replyEgressStats
            );
            NettyReplyDecodedMessageGate replyGate = new NettyReplyDecodedMessageGate(
                    config.replyControlReservationBytes(),
                    config.replyMaxTotalBytes(),
                    outboundConnection,
                    replySequencer
            );
            executionConnection.bindReplyGate(replyGate);
            childChannelRegistry.bindLifecycle(ch, replySequencer.terminationFuture());
            lifecycleBound = true;
            RespRequestDecoder decoder = RespRequestDecoder.withIngressAdmission(
                    config.protocolMaxBulkBytes(),
                    config.protocolMaxArgs(),
                    config.protocolMaxLineBytes(),
                    config.protocolMaxCommandBytes(),
                    inboundMemoryBudget,
                    inboundConnection,
                    replyGate
            );
            decoder.setReadControl(inboundReadCredit);

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
                    .addLast("inboundReadCredit", inboundReadCredit)
                    .addLast("inboundByteAccounting", new InboundByteAccountingHandler(inboundReadCredit))
                    .addLast("respRequestDecoder", decoder)
                    .addLast("executionRequestIngress", new NettyExecutionRequestIngress(executor, replyWriterFactory));
        } finally {
            if (!lifecycleBound) {
                childChannelRegistry.initializationFailed(ch);
            }
        }
    }

    static long perConnectionHardLimit(YierdisServerRuntimeConfig config) {
        Objects.requireNonNull(config, "config");
        long total = saturatedAdd(Math.max(0L, config.protocolMaxCommandBytes()), 48L);
        return saturatedAdd(total, saturatedMultiply(Math.max(0L, config.protocolMaxArgs()), 32L));
    }

    private static int receiveBufferCapacity(YierdisServerRuntimeConfig config) {
        return Math.max(1, Math.min(8 * 1024, config.protocolMaxCommandBytes()));
    }

    private static long saturatedAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private static long saturatedMultiply(long left, long right) {
        if (left <= 0L || right <= 0L) {
            return 0L;
        }
        return left > Long.MAX_VALUE / right ? Long.MAX_VALUE : left * right;
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
