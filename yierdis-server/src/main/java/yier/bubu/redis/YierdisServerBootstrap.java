package yier.bubu.redis;

// Server bootstrap：负责装配 Netty pipeline、DB/off-heap/执行器并管理生命周期，便于测试与工具复用启动/关停逻辑。

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBackend;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocators;

import java.net.InetSocketAddress;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Server bootstrap wrapper that encapsulates wiring and lifecycle management.
 * <p>
 * This allows tests/tools to start/stop the server without duplicating Netty/DB setup logic.
 */
public final class YierdisServerBootstrap implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(YierdisServerBootstrap.class);

    private final ServerConfig config;

    private Channel serverChannel;
    private ScheduledFuture<?> cleanupFuture;

    // Core resources (closed in reverse order).
    private YierdisDb db;
    private NettyCommandExecutor executor;
    private EventExecutorGroup commandGroup;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    // Only used for early failures before DB takes ownership.
    private YierdisOffHeapAllocator earlyOffHeapAllocator;

    private YierdisServerBootstrap(ServerConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    public static YierdisServerBootstrap start(String... args) throws Exception {
        ServerConfig config = ServerConfig.fromArgs(args);
        if (config == null) {
            throw new IllegalArgumentException("No server config (help requested or invalid args)");
        }
        return start(config);
    }

    static YierdisServerBootstrap start(ServerConfig config) throws Exception {
        YierdisServerBootstrap server = new YierdisServerBootstrap(config);
        boolean ok = false;
        try {
            server.startInternal();
            ok = true;
            return server;
        } finally {
            if (!ok) {
                server.close();
            }
        }
    }

    public int port() {
        Channel ch = serverChannel;
        if (ch == null) {
            return config.port;
        }
        if (ch.localAddress() instanceof InetSocketAddress addr) {
            return addr.getPort();
        }
        return config.port;
    }

    public void awaitClose() throws InterruptedException {
        Channel ch = serverChannel;
        if (ch == null) {
            return;
        }
        ch.closeFuture().sync();
    }

    private void startInternal() throws Exception {
        final YierdisOffHeapBackend backend = YierdisOffHeapBackend.fromString(config.offheapBackend);
        log.info("off-heap backend: {} (maxBytes={}, keysOffHeapEnabled={})",
                backend.name().toLowerCase(Locale.ROOT),
                config.offheapMaxBytes,
                config.offheapKeysEnabled);
        log.info("off-heap providers: {}", YierdisOffHeapAllocators.availableProvidersSummary());

        try {
            earlyOffHeapAllocator = YierdisOffHeapAllocators.create(backend, config.offheapMaxBytes);
        } catch (RuntimeException e) {
            log.error("Failed to initialize off-heap backend '{}': {}", backend, e.getMessage());
            throw e;
        }

        db = new YierdisDb(
                earlyOffHeapAllocator,
                config.offheapKeysEnabled,
                config.maxmemoryBytes,
                config.maxmemoryPolicy,
                config.maxmemorySamples,
                config.evictionTimeLimitMillis,
                config.expireCleanupTimeLimitMillis
        );
        // From this point on, db.shutdown() is responsible for closing the allocator.
        earlyOffHeapAllocator = null;

        NettyServerInfoProvider infoProvider = new NettyServerInfoProvider(config);
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db, infoProvider);
        commandGroup = new DefaultEventExecutorGroup(1);
        executor = new NettyCommandExecutor(
                db,
                processor,
                commandGroup.next(),
                config.executorQueueCapacity,
                config.executorQueueMaxBytes,
                config.backpressureHighWatermark,
                config.backpressureLowWatermark,
                config.backpressureBytesHighWatermark,
                config.backpressureBytesLowWatermark,
                config.executorMaxDrainCommands,
                config.executorDrainTimeLimitMillis,
                config.executorSchedulingPolicy,
                config.frameCompactionThresholdBytes,
                config.frameCompactionRatio,
                config.frameCompactionMaxCopyBytes
        );
        infoProvider.bindExecutor(executor);

        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(config.ioThreads);

        // 命令执行器线程是 DB 的唯一访问者（保持单线程命令语义）。
        executor.start();

        if (config.expirationCleanupIntervalMillis > 0) {
            // 关键点：
            // 1) 使用 worker event loop 作为“定时器线程”，避免 command executor 忙碌导致定时器自身无法触发。
            // 2) 通过 executeMaintenance 让 cleanup 在 DB 绑定线程中执行。
            // 3) 通过 coalesce 避免在高压下积累多个 cleanup 请求（fixed-rate catch-up storm）。
            long period = config.expirationCleanupIntervalMillis;
            YierdisDb dbForTask = db;
            NettyCommandExecutor exForTask = executor;
            java.util.concurrent.atomic.AtomicBoolean cleanupPending = new java.util.concurrent.atomic.AtomicBoolean(false);
            cleanupFuture = workerGroup.next().scheduleWithFixedDelay(() -> {
                if (!cleanupPending.compareAndSet(false, true)) {
                    return;
                }
                exForTask.executeMaintenance(() -> {
                    try {
                        dbForTask.cleanupExpired();
                    } catch (Exception e) {
                        log.debug("Expiration cleanup error", e);
                    } finally {
                        cleanupPending.set(false);
                    }
                });
            }, period, period, TimeUnit.MILLISECONDS);
        }

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new YierdisServerChannelInitializer(config, executor));

        serverChannel = bootstrap.bind(config.port).sync().channel();
    }

    @Override
    public void close() {
        // Closing is best-effort: this class is used in tests/tools where leaks are worse than double-close.
        Channel ch = serverChannel;
        if (ch != null) {
            try {
                ch.close().syncUninterruptibly();
            } catch (Throwable ignored) {
                // ignore
            }
        }
        serverChannel = null;

        ScheduledFuture<?> f = cleanupFuture;
        if (f != null) {
            f.cancel(false);
        }
        cleanupFuture = null;

        NettyCommandExecutor ex = executor;
        if (ex != null) {
            try {
                ex.shutdownGracefully().syncUninterruptibly();
            } catch (Throwable ignored) {
                // ignore
            }
        }

        YierdisDb d = db;
        if (d != null) {
            try {
                if (ex != null) {
                    ex.executor().submit(d::shutdown).syncUninterruptibly();
                } else {
                    d.shutdown();
                }
            } catch (Throwable ignored) {
                // ignore
            }
        }
        db = null;
        executor = null;

        EventExecutorGroup cg = commandGroup;
        if (cg != null) {
            cg.shutdownGracefully().syncUninterruptibly();
        }
        commandGroup = null;

        EventLoopGroup boss = bossGroup;
        if (boss != null) {
            boss.shutdownGracefully().syncUninterruptibly();
        }
        bossGroup = null;

        EventLoopGroup workers = workerGroup;
        if (workers != null) {
            workers.shutdownGracefully().syncUninterruptibly();
        }
        workerGroup = null;

        YierdisOffHeapAllocator allocator = earlyOffHeapAllocator;
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception ignored) {
                // ignore
            }
        }
        earlyOffHeapAllocator = null;
    }
}
