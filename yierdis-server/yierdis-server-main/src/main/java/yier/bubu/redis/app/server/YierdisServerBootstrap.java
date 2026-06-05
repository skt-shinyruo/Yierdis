package yier.bubu.redis.app.server;

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
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;
import yier.bubu.redis.command.defaults.DefaultCommandModules;
import yier.bubu.redis.command.api.SlowCommandGovernor;
import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.command.kernel.YierdisCommandProcessorOptions;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.RedisReplyWriterFactory;
import yier.bubu.redis.execution.engine.DefaultYierdisEngine;
import yier.bubu.redis.execution.engine.YierdisEngine;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.execution.executor.CommandExecutorConfig;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.protocol.resp.RespReplyWriterFactory;
import yier.bubu.redis.storage.memory.YierdisDbEngineFactory;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.runtime.embedded.YierdisInstanceMaintenance;
import yier.bubu.redis.runtime.embedded.YierdisInstanceObservability;
import yier.bubu.redis.runtime.embedded.YierdisInstanceRuntimeAccess;

import java.net.InetSocketAddress;
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
    private YierdisEngine engine;
    private CommandExecutor<NettyExecutionConnection> executor;
    private NettyServerInfoProvider infoProvider;
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
        ForeignMemoryAutoModules.ensureFfmAvailable();
        log.info("native memory backend: foreign (JDK 25 FFM)");
        int databases = Math.max(1, runtimeConfig.databases());
        YierdisInstanceConfig.MaxmemoryScope scope =
                runtimeConfig.maxmemoryScope() == YierdisServerRuntimeConfig.MaxmemoryScope.PER_DB
                        ? YierdisInstanceConfig.MaxmemoryScope.PER_DB
                        : YierdisInstanceConfig.MaxmemoryScope.GLOBAL;
        YierdisInstanceConfig.Builder instanceConfig = YierdisInstanceConfig.builder()
                .databases(databases)
                .maxmemoryBytes(runtimeConfig.maxmemoryBytes())
                .maxmemoryScope(scope)
                .maxmemoryPolicy(runtimeConfig.maxmemoryPolicy())
                .maxmemorySamples(runtimeConfig.maxmemorySamples())
                .evictionTimeLimitMillis(runtimeConfig.evictionTimeLimitMillis())
                .expireCleanupTimeLimitMillis(runtimeConfig.expireCleanupTimeLimitMillis())
                .nativeDefragEnabled(runtimeConfig.nativeDefragEnabled())
                .nativeDefragMaxMoveBytes(runtimeConfig.nativeDefragMaxMoveBytes())
                .nativeDefragMaxObjects(runtimeConfig.nativeDefragMaxObjects())
                .nativeDefragTimeLimitMillis(runtimeConfig.nativeDefragTimeLimitMillis());
        configureDefaultDbEngineFactory(instanceConfig, scope, runtimeConfig);
        instance = YierdisInstance.create(instanceConfig.build());
        YierdisInstanceRuntimeAccess runtimeAccess = instance.runtimeAccess();
        Runnable maintenanceTick = new YierdisInstanceMaintenance(instance)::maintenanceTick;
        YierdisInstanceObservability observability = instance.observability();

        infoProvider = new NettyServerInfoProvider(runtimeConfig);
        infoProvider.bindObservability(observability);
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
        YierdisCommandProcessorOptions commandProcessorOptions = YierdisCommandProcessorOptions.builder()
                .changeObserver(RuntimeChangeSinkCommandChangeObserver.fromSink(instance.config().changeSink()))
                .build();
        YierdisEngine commandEngine = new DefaultYierdisEngine(
                commandProcessorOptions,
                maintenanceTick,
                DefaultCommandModules.create(dbRouter(instance), infoProvider, slowGovernor),
                new ServerCommandModule(infoProvider)
        );
        engine = commandEngine;
        commandGroup = new DefaultEventExecutorGroup(1);
        RedisReplyWriterFactory replyWriterFactory = new RespReplyWriterFactory();
        CommandExecutorConfig executorConfig = CommandExecutorConfigs.from(runtimeConfig);
        executor = new CommandExecutor<>(
                runtimeAccess::bindToCurrentThread,
                commandEngine::execute,
                commandGroup.next(),
                replyWriterFactory,
                new NettyExecutionIoAdapter(),
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
            CommandExecutor<NettyExecutionConnection> exForTask = executor;
            java.util.concurrent.atomic.AtomicBoolean cleanupPending = new java.util.concurrent.atomic.AtomicBoolean(false);
            cleanupFuture = workerGroup.next().scheduleWithFixedDelay(() -> {
                if (!cleanupPending.compareAndSet(false, true)) {
                    return;
                }
                exForTask.executeMaintenance(() -> {
                    try {
                        commandEngine.maintenanceTick();
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
                .childHandler(new YierdisServerChannelInitializer(runtimeConfig, executor, replyWriterFactory));

        serverChannel = bootstrap.bind(runtimeConfig.port()).sync().channel();
    }

    private static void configureDefaultDbEngineFactory(
            YierdisInstanceConfig.Builder instanceConfig,
            YierdisInstanceConfig.MaxmemoryScope scope,
            YierdisServerRuntimeConfig runtimeConfig
    ) {
        NativeDefragOptions nativeDefragOptions = nativeDefragOptions(runtimeConfig);
        if (scope == YierdisInstanceConfig.MaxmemoryScope.PER_DB) {
            instanceConfig.engineFactory(new YierdisDbEngineFactory(nativeDefragOptions));
            return;
        }
        YierdisFfmMemoryRuntime memoryRuntime = new YierdisFfmMemoryRuntime("instance");
        instanceConfig
                .engineFactoryBinding(new YierdisInstanceConfig.EngineFactoryBinding(
                        new YierdisDbEngineFactory(memoryRuntime, nativeDefragOptions),
                        memoryRuntime
                ));
    }

    private static NativeDefragOptions nativeDefragOptions(YierdisServerRuntimeConfig runtimeConfig) {
        if (!runtimeConfig.nativeDefragEnabled()) {
            return null;
        }
        return new NativeDefragOptions(
                runtimeConfig.nativeDefragMaxMoveBytes(),
                runtimeConfig.nativeDefragMaxObjects(),
                TimeUnit.MILLISECONDS.toNanos(runtimeConfig.nativeDefragTimeLimitMillis())
        );
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

        CommandExecutor<NettyExecutionConnection> ex = executor;
        if (ex != null) {
            try {
                ex.shutdownGracefully().join();
            } catch (Throwable t) {
                failure = recordCloseFailure(failure, t);
            }
        }

        YierdisEngine eng = engine;
        if (eng != null) {
            try {
                eng.close();
            } catch (Throwable t) {
                failure = recordCloseFailure(failure, t);
            }
        }
        engine = null;

        YierdisInstance inst = instance;
        if (inst != null) {
            try {
                closeRuntimeAccess(ex, inst.runtimeAccess());
            } catch (Throwable t) {
                failure = recordCloseFailure(failure, t);
            }
        }
        instance = null;
        executor = null;
        infoProvider = null;

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

        rethrowIfNeeded(failure);
    }

    NettyServerInfoProvider infoProviderForTests() {
        return infoProvider;
    }

    private static void closeRuntimeAccess(CommandExecutor<?> executor, YierdisInstanceRuntimeAccess runtimeAccess) throws Throwable {
        Objects.requireNonNull(runtimeAccess, "runtimeAccess");
        if (executor == null) {
            runtimeAccess.close();
            return;
        }

        try {
            executor.executeOwnerTask(runtimeAccess::close).join();
        } catch (java.util.concurrent.CompletionException e) {
            if (e.getCause() != null) {
                throw e.getCause();
            }
            throw e;
        }
    }

    private static YierdisDbRouter dbRouter(YierdisInstance instance) {
        Objects.requireNonNull(instance, "instance");
        DbEngine[] dbViews = instance.engines();
        return new YierdisDbRouter() {
            @Override
            public DbEngine dbFor(yier.bubu.redis.execution.api.DbIndexSession session) {
                if (dbViews.length == 0) {
                    throw new IllegalStateException("no dbs");
                }
                int idx = session == null ? 0 : session.dbIndex();
                if (idx < 0 || idx >= dbViews.length) {
                    idx = 0;
                }
                return dbViews[idx];
            }

            @Override
            public int databases() {
                return Math.max(1, dbViews.length);
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
