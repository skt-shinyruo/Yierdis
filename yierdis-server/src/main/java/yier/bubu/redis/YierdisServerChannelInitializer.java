package yier.bubu.redis;

// Netty server 连接初始化器：显式装配 decode→handle 的 pipeline，并在连接建立时绑定连接态（协议会话与执行器调度状态）。

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import yier.bubu.redis.args.YierdisServerRuntimeConfig;
import yier.bubu.redis.protocol.netty.CustomRequestDecoder;

import java.util.Objects;

final class YierdisServerChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final YierdisServerRuntimeConfig config;
    private final NettyCommandExecutor executor;

    YierdisServerChannelInitializer(YierdisServerRuntimeConfig config, NettyCommandExecutor executor) {
        this.config = Objects.requireNonNull(config, "config");
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        // 统一连接态入口：会话（SELECT/MULTI/...）、运行时（背压/统计/closing）与调度状态在同一 context 内初始化。
        ServerConnectionContext.getOrCreate(
                ch,
                config.transactionQueueMaxCommands(),
                config.transactionQueueMaxBytes()
        );

        ch.pipeline()
                .addLast("writeBufferBackpressure", new WriteBufferBackpressureHandler(executor))
                .addLast("customRequestDecoder", new CustomRequestDecoder(
                        config.protocolMaxBulkBytes(),
                        config.protocolMaxArgs(),
                        config.protocolMaxLineBytes()
                ))
                .addLast("protocolErrorReply", new ProtocolErrorReplyHandler(executor))
                .addLast("commandHandler", new YierdisFastCommandHandler(executor));
    }

    /**
     * Disables autoRead when the channel becomes unwritable (outbound buffer high watermark), and asks the
     * executor to re-evaluate autoRead when it becomes writable again.
     */
    private static final class WriteBufferBackpressureHandler extends io.netty.channel.ChannelInboundHandlerAdapter {
        private final NettyCommandExecutor executor;

        private WriteBufferBackpressureHandler(NettyCommandExecutor executor) {
            this.executor = Objects.requireNonNull(executor, "executor");
        }

        @Override
        public void channelWritabilityChanged(io.netty.channel.ChannelHandlerContext ctx) throws Exception {
            if (ctx != null) {
                if (!ctx.channel().isWritable()) {
                    // Use the same autoRead flag as executor backpressure; enable is guarded by ch.isWritable().
                    executor.disableAutoRead(ctx.channel());
                } else {
                    executor.onChannelWritable(ctx.channel());
                }
            }
            super.channelWritabilityChanged(ctx);
        }
    }
}
