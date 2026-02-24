package yier.bubu.redis;

// Netty server 连接初始化器：显式装配 decode→handle 的 pipeline，并在连接建立时绑定连接态（协议会话与执行器调度状态）。

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import yier.bubu.redis.protocol.netty.CustomRequestDecoder;

import java.util.Objects;

final class YierdisServerChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final ServerConfig config;
    private final NettyCommandExecutor executor;

    YierdisServerChannelInitializer(ServerConfig config, NettyCommandExecutor executor) {
        this.config = Objects.requireNonNull(config, "config");
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        // server 会话状态（SELECT/MULTI/...）与运行时状态（背压/统计/closing 等）分离绑定。
        ServerSessionState.getOrCreate(
                ch,
                config.transactionQueueMaxCommands,
                config.transactionQueueMaxBytes
        );
        ServerRuntimeState.getOrCreate(ch);
        // 执行器调度状态（server 私有，避免放入 ConnectionContext）
        NettyExecutorChannelState.getOrCreate(ch);

        ch.pipeline()
                .addLast("writeBufferBackpressure", new WriteBufferBackpressureHandler(executor))
                .addLast("customRequestDecoder", new CustomRequestDecoder(
                        config.protocolMaxBulkBytes,
                        config.protocolMaxArgs,
                        config.protocolMaxLineBytes
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
