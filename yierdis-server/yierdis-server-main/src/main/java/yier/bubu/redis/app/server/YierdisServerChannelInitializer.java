package yier.bubu.redis.app.server;

// Netty server 连接初始化器：显式装配 decode→handle 的 pipeline，并在连接建立时绑定连接态（协议会话与执行器调度状态）。

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;
import yier.bubu.redis.execution.api.ReplyWriterFactory;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.protocol.resp.netty.RespCommandAdapter;
import yier.bubu.redis.protocol.resp.netty.RespProtocolErrorReplyHandler;
import yier.bubu.redis.protocol.resp.netty.RespRequestDecoder;

import java.util.Objects;

final class YierdisServerChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final YierdisServerRuntimeConfig config;
    private final CommandExecutor<NettyExecutionConnection> executor;
    private final ReplyWriterFactory replyWriterFactory;

    YierdisServerChannelInitializer(
            YierdisServerRuntimeConfig config,
            CommandExecutor<NettyExecutionConnection> executor,
            ReplyWriterFactory replyWriterFactory
    ) {
        this.config = Objects.requireNonNull(config, "config");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.replyWriterFactory = Objects.requireNonNull(replyWriterFactory, "replyWriterFactory");
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        NettyExecutionConnection.getOrCreate(
                ch,
                config.transactionQueueMaxCommands(),
                config.transactionQueueMaxBytes()
        );

        ch.pipeline()
                .addLast("writeBufferBackpressure", new WriteBufferBackpressureHandler(executor))
                .addLast("respRequestDecoder", new RespRequestDecoder(
                        config.protocolMaxBulkBytes(),
                        config.protocolMaxArgs(),
                        config.protocolMaxLineBytes()
                ))
                .addLast("respCommandAdapter", new RespCommandAdapter())
                .addLast("respProtocolErrorReply", new RespProtocolErrorReplyHandler(replyWriterFactory))
                .addLast("commandHandler", new YierdisFastCommandHandler(executor, replyWriterFactory));
    }

    /**
     * Disables autoRead when the channel becomes unwritable (outbound buffer high watermark), and asks the
     * executor to re-evaluate autoRead when it becomes writable again.
     */
    private static final class WriteBufferBackpressureHandler extends io.netty.channel.ChannelInboundHandlerAdapter {
        private final CommandExecutor<NettyExecutionConnection> executor;

        private WriteBufferBackpressureHandler(CommandExecutor<NettyExecutionConnection> executor) {
            this.executor = Objects.requireNonNull(executor, "executor");
        }

        @Override
        public void channelWritabilityChanged(io.netty.channel.ChannelHandlerContext ctx) throws Exception {
            if (ctx != null) {
                NettyExecutionConnection connection = NettyExecutionConnection.get(ctx.channel());
                if (!ctx.channel().isWritable()) {
                    executor.onTransportUnwritable(connection);
                } else if (connection != null) {
                    executor.onTransportWritable(connection);
                }
            }
            super.channelWritabilityChanged(ctx);
        }
    }
}
