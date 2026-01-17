package yier.bubu.redis;

// Netty server 连接初始化器：显式装配 decode→handle 的 pipeline，并在连接建立时绑定连接态（协议会话与执行器调度状态）。

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import yier.bubu.redis.protocol.netty.ConnectionContext;
import yier.bubu.redis.protocol.netty.RespCommandDecoder;

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
        // 协议会话 SSOT（RESP2/RESP3 协商等）
        ConnectionContext session = ConnectionContext.getOrCreate(ch);
        // server 连接运行时状态（背压/统计/closing 等）
        ServerConnectionState.getOrCreate(ch, session);
        // 执行器调度状态（server 私有，避免放入 ConnectionContext）
        NettyExecutorChannelState.getOrCreate(ch);

        ch.pipeline()
                .addLast("respCommandDecoder", new RespCommandDecoder(
                        config.protocolMaxBulkBytes,
                        config.protocolMaxArgs,
                        config.protocolMaxLineBytes
                ))
                .addLast("commandHandler", new YierdisFastCommandHandler(executor));
    }
}
