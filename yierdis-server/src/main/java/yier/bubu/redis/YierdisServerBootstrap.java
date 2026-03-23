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
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yier.bubu.redis.args.YierdisCliException;
import yier.bubu.redis.args.YierdisServerRuntimeConfig;
import yier.bubu.redis.command.YierdisDbRouter;
import yier.bubu.redis.command.SlowCommandGovernor;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBackend;
import yier.bubu.redis.db.memory.api.YierdisOffHeapAllocators;
import yier.bubu.redis.db.memory.api.YierdisOffHeapBackendUnavailableException;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.protocol.v1.JsonLineReplyWriterFactory;
import yier.bubu.redis.runtime.YierdisInstance;
import yier.bubu.redis.runtime.YierdisInstanceConfig;
import yier.bubu.redis.runtime.YierdisInstanceRuntimeAccess;

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
    private final YierdisServerRuntimeConfig runtimeConfig;

    private Channel serverChannel;
    private ScheduledFuture<?> cleanupFuture;

    // Core resources (closed in reverse order).
    private YierdisInstance instance;
    private DbEngine[] engines;
    private YierdisOffHeapAllocator offHeapAllocator;
    private NettyCommandExecutor executor;
    private EventExecutorGroup commandGroup;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    private YierdisServerBootstrap(ServerConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.runtimeConfig = config.runtimeConfig();
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
            return runtimeConfig.port();
        }
        if (ch.localAddress() instanceof InetSocketAddress addr) {
            return addr.getPort();
        }
        return runtimeConfig.port();
    }

    public void awaitClose() throws InterruptedException {
        Channel ch = serverChannel;
        if (ch == null) {
            return;
        }
        ch.closeFuture().sync();
    }

    private void startInternal() throws Exception {
        final YierdisOffHeapBackend backend = YierdisOffHeapBackend.fromString(runtimeConfig.offheapBackend().argvValue());
        log.info("off-heap backend: {} (maxBytes={}, keysOffHeapEnabled={})",
                backend.name().toLowerCase(Locale.ROOT),
                runtimeConfig.offheapMaxBytes(),
                runtimeConfig.offheapKeysEnabled());
        log.info("off-heap providers: {}", YierdisOffHeapAllocators.availableProvidersSummary());

        try {
            offHeapAllocator = YierdisOffHeapAllocators.create(backend, runtimeConfig.offheapMaxBytes());
            if (backend != YierdisOffHeapBackend.NONE && runtimeConfig.offheapMaxBytes() == 0) {
                log.warn("off-heap backend '{}' is enabled but offheapMaxBytes=0 (no hard cap). "
                                + "If you rely on maxmemoryBytes, consider setting --offheapMaxBytes to avoid surprises.",
                        backend.name().toLowerCase(Locale.ROOT));
            }
        } catch (YierdisOffHeapBackendUnavailableException e) {
            // 可预期配置错误：避免输出长堆栈，由 CLI 统一以稳定退出码退出。
            log.error("Failed to initialize off-heap backend '{}': {}", backend, e.getMessage());
            throw YierdisCliException.userError(e.getMessage(), e);
        } catch (RuntimeException e) {
            log.error("Failed to initialize off-heap backend '{}': {}", backend, e.getMessage());
            throw e;
        }

        boolean perDbScope = runtimeConfig.maxmemoryScope() == YierdisServerRuntimeConfig.MaxmemoryScope.PER_DB;
        int databases = Math.max(1, runtimeConfig.databases());
        YierdisInstanceConfig.MaxmemoryScope scope =
                perDbScope ? YierdisInstanceConfig.MaxmemoryScope.PER_DB : YierdisInstanceConfig.MaxmemoryScope.GLOBAL;
        instance = YierdisInstance.create(YierdisInstanceConfig.builder()
                .databases(databases)
                .offHeapAllocator(offHeapAllocator)
                .ownsOffHeapAllocator(false)
                .offHeapKeysEnabled(runtimeConfig.offheapKeysEnabled())
                .maxmemoryBytes(runtimeConfig.maxmemoryBytes())
                .maxmemoryScope(scope)
                .maxmemoryPolicy(runtimeConfig.maxmemoryPolicy().argvValue())
                .maxmemorySamples(runtimeConfig.maxmemorySamples())
                .evictionTimeLimitMillis(runtimeConfig.evictionTimeLimitMillis())
                .expireCleanupTimeLimitMillis(runtimeConfig.expireCleanupTimeLimitMillis())
                .build());
        YierdisInstanceRuntimeAccess runtimeAccess = instance.runtimeAccess();
        engines = instance.engines();

        NettyServerInfoProvider infoProvider = new NettyServerInfoProvider(runtimeConfig);
        infoProvider.bindEngines(engines);
        SlowCommandGovernor slowGovernor = new SlowCommandGovernor() {
            private final long timeBudgetNanos = runtimeConfig.keysTimeBudgetMillis() <= 0
                    ? 0
                    : TimeUnit.MILLISECONDS.toNanos(runtimeConfig.keysTimeBudgetMillis());

            @Override
            public long keysTimeBudgetNanos(CommandContext ctx) {
                return timeBudgetNanos;
            }

            @Override
            public int keysMaxResults(CommandContext ctx) {
                return runtimeConfig.keysMaxResults();
            }
        };
        YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(
                dbRouter(instance),
                infoProvider,
                slowGovernor,
                new ServerCommandModule(infoProvider)
        );
        commandGroup = new DefaultEventExecutorGroup(1);
        NettyCommandExecutorConfig executorConfig = NettyCommandExecutorConfig.from(runtimeConfig);
        executor = new NettyCommandExecutor(
                runtimeAccess::bindToCurrentThread,
                processor,
                commandGroup.next(),
                new JsonLineReplyWriterFactory(),
                executorConfig
        );
        infoProvider.bindExecutor(executor);

        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(runtimeConfig.ioThreads());

        // 命令执行器线程是 DB 的唯一访问者（保持单线程命令语义）。
        executor.start();

        if (runtimeConfig.cleanupIntervalMillis() > 0) {
            // 关键点：
            // 1) 使用 worker event loop 作为“定时器线程”，避免 command executor 忙碌导致定时器自身无法触发。
            // 2) 通过 executeMaintenance 让 cleanup 在 DB 绑定线程中执行。
            // 3) 通过 coalesce 避免在高压下积累多个 cleanup 请求（fixed-rate catch-up storm）。
            long period = runtimeConfig.cleanupIntervalMillis();
            NettyCommandExecutor exForTask = executor;
            java.util.concurrent.atomic.AtomicBoolean cleanupPending = new java.util.concurrent.atomic.AtomicBoolean(false);
            cleanupFuture = workerGroup.next().scheduleWithFixedDelay(() -> {
                if (!cleanupPending.compareAndSet(false, true)) {
                    return;
                }
                exForTask.executeMaintenance(() -> {
                    try {
                        runtimeAccess.maintenanceTick();
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
                .childHandler(new YierdisServerChannelInitializer(runtimeConfig, executor));

        serverChannel = bootstrap.bind(runtimeConfig.port()).sync().channel();
    }

    @Override
    public void close() {
        // Closing is best-effort: this class is used in tests/tools where leaks are worse than double-close.
        Throwable failure = null;

        Channel ch = serverChannel;
        if (ch != null) {
            try {
                ch.close().syncUninterruptibly();
            } catch (Throwable t) {
                failure = recordCloseFailure(failure, t);
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
            } catch (Throwable t) {
                failure = recordCloseFailure(failure, t);
            }
        }

        YierdisInstance inst = instance;
        if (inst != null) {
            try {
                closeRuntimeAccess(ex, inst.runtimeAccess());
            } catch (Throwable t) {
                failure = recordCloseFailure(failure, t);
            }
        }
        instance = null;
        engines = null;
        executor = null;

        EventExecutorGroup cg = commandGroup;
        if (cg != null) {
            try {
                cg.shutdownGracefully().syncUninterruptibly();
            } catch (Throwable t) {
                failure = recordCloseFailure(failure, t);
            }
        }
        commandGroup = null;

        EventLoopGroup boss = bossGroup;
        if (boss != null) {
            try {
                boss.shutdownGracefully().syncUninterruptibly();
            } catch (Throwable t) {
                failure = recordCloseFailure(failure, t);
            }
        }
        bossGroup = null;

        EventLoopGroup workers = workerGroup;
        if (workers != null) {
            try {
                workers.shutdownGracefully().syncUninterruptibly();
            } catch (Throwable t) {
                failure = recordCloseFailure(failure, t);
            }
        }
        workerGroup = null;

        YierdisOffHeapAllocator allocator = offHeapAllocator;
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Throwable t) {
                failure = recordCloseFailure(failure, t);
            }
        }
        offHeapAllocator = null;

        rethrowIfNeeded(failure);
    }

    private static void closeRuntimeAccess(NettyCommandExecutor executor, YierdisInstanceRuntimeAccess runtimeAccess) throws Throwable {
        Objects.requireNonNull(runtimeAccess, "runtimeAccess");
        if (executor == null) {
            runtimeAccess.close();
            return;
        }

        Future<?> closeFuture = executor.executor().submit(() -> {
            runtimeAccess.close();
            return null;
        });
        closeFuture.awaitUninterruptibly();
        Throwable cause = closeFuture.cause();
        if (cause != null) {
            throw cause;
        }
    }

    private static YierdisDbRouter dbRouter(YierdisInstance instance) {
        Objects.requireNonNull(instance, "instance");
        DbEngine[] engines = instance.engines();
        return new YierdisDbRouter() {
            @Override
            public DbEngine dbFor(yier.bubu.redis.contract.DbIndexProvider dbIndexProvider) {
                if (engines.length == 0) {
                    throw new IllegalStateException("no dbs");
                }
                int idx = dbIndexProvider == null ? 0 : dbIndexProvider.dbIndex();
                if (idx < 0 || idx >= engines.length) {
                    idx = 0;
                }
                return engines[idx];
            }

            @Override
            public int databases() {
                return Math.max(1, engines.length);
            }
        };
    }

    private static Throwable recordCloseFailure(Throwable current, Throwable next) {
        if (next == null) {
            return current;
        }
        if (current == null) {
            return next;
        }
        current.addSuppressed(next);
        return current;
    }

    private static void rethrowIfNeeded(Throwable failure) {
        if (failure == null) {
            return;
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new IllegalStateException("close failed", failure);
    }
}
