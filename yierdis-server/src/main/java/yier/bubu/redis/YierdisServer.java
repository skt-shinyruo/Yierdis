package yier.bubu.redis;

import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocators;
import yier.bubu.redis.protocol.RespCommandDecoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public final class YierdisServer {
    private static final Logger log = LoggerFactory.getLogger(YierdisServer.class);

    public static void main(String[] args) throws Exception {
        ServerConfig config = ServerConfig.fromArgs(args);

        final YierdisOffHeapAllocator offHeapAllocator =
                YierdisOffHeapAllocators.create(config.offheapBackend, config.offheapMaxBytes);
        final YierdisDb db = new YierdisDb(
                offHeapAllocator,
                config.maxmemoryBytes,
                config.maxmemoryPolicy,
                config.maxmemorySamples,
                config.evictionTimeLimitMillis,
                config.expireCleanupTimeLimitMillis
        );
        final YierdisFastCommandProcessor commandProcessor = new YierdisFastCommandProcessor(db);
        final EventExecutorGroup commandGroup = new DefaultEventExecutorGroup(1);
        final NettyCommandExecutor executor = new NettyCommandExecutor(
                db,
                commandProcessor,
                commandGroup.next(),
                config.executorQueueCapacity,
                config.backpressureHighWatermark,
                config.backpressureLowWatermark,
                config.executorMaxDrainCommands,
                config.executorDrainTimeLimitMillis
        );


         EventLoopGroup bossGroup = new NioEventLoopGroup(1);
         EventLoopGroup workerGroup = new NioEventLoopGroup(config.ioThreads);
         ScheduledFuture<?> cleanupFuture = null;
         try {
             // 命令执行器线程是 DB 的唯一访问者（保持单线程命令语义）。
             executor.start();

            if (config.expirationCleanupIntervalMillis > 0) {
                long period = config.expirationCleanupIntervalMillis;
                cleanupFuture = executor.executor().scheduleAtFixedRate(() -> {
                    try {
                        db.cleanupExpired();
                    } catch (Exception e) {
                        log.debug("Expiration cleanup error", e);
                    }
                }, period, period, TimeUnit.MILLISECONDS);
            }

            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast("respCommandDecoder", new RespCommandDecoder())
                                    .addLast("commandHandler", new YierdisFastCommandHandler(commandProcessor, executor));
                        }
                    });

            Channel serverChannel = bootstrap.bind(config.port).sync().channel();
            log.info("yierdis started on 0.0.0.0:{} (RESP2 default; supports HELLO 3 / RESP3 + inline)", config.port);
            serverChannel.closeFuture().sync();
        } finally {
            if (cleanupFuture != null) {
                cleanupFuture.cancel(false);
            }
            executor.close();
            db.shutdown();
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            commandGroup.shutdownGracefully();
        }
    }

    private YierdisServer() {
    }
}
