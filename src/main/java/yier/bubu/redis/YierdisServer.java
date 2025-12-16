package yier.bubu.redis;

import yier.bubu.redis.command.CommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.protocol.RespDecoder;
import yier.bubu.redis.protocol.RespEncoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public final class YierdisServer {
    private static final Logger log = LoggerFactory.getLogger(YierdisServer.class);

    public static void main(String[] args) throws Exception {
        ServerConfig config = ServerConfig.fromArgs(args);

        final YierdisDb db = new YierdisDb();
        if (config.expirationCleanupIntervalMillis > 0) {
            db.startExpirationCleanup(config.expirationCleanupIntervalMillis, TimeUnit.MILLISECONDS);
        }

        final CommandProcessor commandProcessor = new CommandProcessor(db);

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast("respDecoder", new RespDecoder())
                                    .addLast("respEncoder", new RespEncoder())
                                    .addLast("commandHandler", new YierdisCommandHandler(commandProcessor));
                        }
                    });

            Channel serverChannel = bootstrap.bind(config.port).sync().channel();
            log.info("yierdis started on 0.0.0.0:{} (RESP2)", config.port);
            serverChannel.closeFuture().sync();
        } finally {
            db.shutdown();
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private YierdisServer() {
    }
}
