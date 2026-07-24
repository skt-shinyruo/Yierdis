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
import yier.bubu.redis.command.api.SlowCommandGovernor;
import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.RedisReplyWriterFactory;
import yier.bubu.redis.execution.engine.DefaultYierdisEngine;
import yier.bubu.redis.execution.engine.YierdisEngine;
import yier.bubu.redis.execution.executor.CommandExecutionEngine;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.execution.executor.CommandExecutorConfig;
import yier.bubu.redis.execution.executor.SerialOwnerExecutor;
import yier.bubu.redis.memory.api.StableMemoryBackendFactory;
import yier.bubu.redis.memory.foreign.YierdisFfmStableMemoryBackend;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngine;
import yier.bubu.redis.storage.api.DbEngineFactory;
import yier.bubu.redis.protocol.resp.RespReplyWriterFactory;
import yier.bubu.redis.protocol.resp.netty.InboundMemoryBudget;
import yier.bubu.redis.storage.memory.YierdisDbEngineFactory;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;
import yier.bubu.redis.runtime.embedded.YierdisInstanceMaintenance;
import yier.bubu.redis.runtime.embedded.YierdisInstanceObservability;
import yier.bubu.redis.runtime.embedded.YierdisInstanceRuntimeAccess;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * Server bootstrap wrapper that encapsulates wiring and lifecycle management.
 * <p>
 * This allows tests/tools to start/stop the server without duplicating Netty/DB setup logic.
 */
public final class YierdisServerBootstrap implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(YierdisServerBootstrap.class);

    enum LifecycleState {
        STARTING,
        RUNNING,
        CLOSING,
        CLOSED,
        FAILED
    }

    private final ServerConfig config;
    private final YierdisServerRuntimeConfig runtimeConfig;
    private final UnaryOperator<CommandExecutionEngine> commandEngineDecorator;
    private final Consumer<YierdisInstanceConfig.Builder> instanceConfigCustomizer;
    private final Object lifecycleLock = new Object();
    private volatile LifecycleState lifecycleState = LifecycleState.STARTING;
    private CompletableFuture<Void> closeAttempt;

    private Channel serverChannel;
    private ScheduledFuture<?> cleanupFuture;

    // Core resources (closed in reverse order).
    private YierdisInstance instance;
    private YierdisEngine engine;
    private CommandExecutor<NettyExecutionConnection> executor;
    private NettyServerInfoProvider infoProvider;
    private InboundMemoryBudget inboundMemoryBudget;
    private OutboundMemoryBudget outboundMemoryBudget;
    private ChildChannelRegistry childChannelRegistry;
    private ReplyEgressStats replyEgressStats;
    private EventExecutorGroup commandGroup;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    private YierdisServerBootstrap(ServerConfig config) {
        this(config, UnaryOperator.identity(), ignored -> { });
    }

    private YierdisServerBootstrap(
            ServerConfig config,
            UnaryOperator<CommandExecutionEngine> commandEngineDecorator
    ) {
        this(config, commandEngineDecorator, ignored -> { });
    }

    private YierdisServerBootstrap(
            ServerConfig config,
            UnaryOperator<CommandExecutionEngine> commandEngineDecorator,
            Consumer<YierdisInstanceConfig.Builder> instanceConfigCustomizer
    ) {
        this.config = Objects.requireNonNull(config, "config");
        this.runtimeConfig = config.runtimeConfig();
        this.commandEngineDecorator = Objects.requireNonNull(commandEngineDecorator, "commandEngineDecorator");
        this.instanceConfigCustomizer = Objects.requireNonNull(instanceConfigCustomizer, "instanceConfigCustomizer");
    }

    public static YierdisServerBootstrap start(String... args) throws Exception {
        ServerConfig config = ServerConfig.fromArgs(args);
        if (config == null) {
            throw new IllegalArgumentException("No server config (help requested or invalid args)");
        }
        return start(config);
    }

    static YierdisServerBootstrap start(ServerConfig config) throws Exception {
        return start(config, UnaryOperator.identity());
    }

    static YierdisServerBootstrap startForTests(
            UnaryOperator<CommandExecutionEngine> commandEngineDecorator,
            String... args
    ) throws Exception {
        ServerConfig config = ServerConfig.fromArgs(args);
        if (config == null) {
            throw new IllegalArgumentException("No server config (help requested or invalid args)");
        }
        return start(config, commandEngineDecorator);
    }

    static YierdisServerBootstrap startForTests(
            UnaryOperator<CommandExecutionEngine> commandEngineDecorator,
            Consumer<YierdisInstanceConfig.Builder> instanceConfigCustomizer,
            String... args
    ) throws Exception {
        ServerConfig config = ServerConfig.fromArgs(args);
        if (config == null) {
            throw new IllegalArgumentException("No server config (help requested or invalid args)");
        }
        return start(config, commandEngineDecorator, instanceConfigCustomizer);
    }

    private static YierdisServerBootstrap start(
            ServerConfig config,
            UnaryOperator<CommandExecutionEngine> commandEngineDecorator
    ) throws Exception {
        return start(config, commandEngineDecorator, ignored -> { });
    }

    private static YierdisServerBootstrap start(
            ServerConfig config,
            UnaryOperator<CommandExecutionEngine> commandEngineDecorator,
            Consumer<YierdisInstanceConfig.Builder> instanceConfigCustomizer
    ) throws Exception {
        YierdisServerBootstrap server = new YierdisServerBootstrap(
                config,
                commandEngineDecorator,
                instanceConfigCustomizer
        );
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
                .defrag(new DbDefragConfig(
                        runtimeConfig.nativeDefragEnabled(),
                        runtimeConfig.nativeDefragMaxMoveBytes(),
                        runtimeConfig.nativeDefragMaxObjects(),
                        runtimeConfig.nativeDefragTimeLimitMillis()
                ));
        configureDefaultDbEngineFactory(instanceConfig, runtimeConfig);
        instanceConfigCustomizer.accept(instanceConfig);
        instance = YierdisInstance.create(instanceConfig.build());
        YierdisInstanceRuntimeAccess runtimeAccess = instance.runtimeAccess();
        Runnable maintenanceTick = new YierdisInstanceMaintenance(instance)::maintenanceTick;
        YierdisInstanceObservability observability = instance.observability();

        infoProvider = new NettyServerInfoProvider(runtimeConfig);
        infoProvider.bindLifecycleState(() -> lifecycleState.name());
        infoProvider.bindObservability(observability);
        inboundMemoryBudget = new InboundMemoryBudget(runtimeConfig.protocolGlobalInFlightBytes());
        infoProvider.bindInboundMemoryBudget(inboundMemoryBudget);
        outboundMemoryBudget = new OutboundMemoryBudget(runtimeConfig.replyGlobalCapacityBytes());
        childChannelRegistry = new ChildChannelRegistry(runtimeConfig.maxClients());
        replyEgressStats = new ReplyEgressStats();
        infoProvider.bindOutboundMemoryBudget(outboundMemoryBudget);
        infoProvider.bindChildChannelRegistry(childChannelRegistry);
        infoProvider.bindReplyEgressStats(replyEgressStats);
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
        YierdisFastCommandProcessor commandProcessor = ServerCommandComposition.createProcessor(
                dbRouter(instance),
                infoProvider,
                slowGovernor
        );
        YierdisEngine commandEngine = new DefaultYierdisEngine(
                commandProcessor,
                maintenanceTick
        );
        engine = commandEngine;
        CommandExecutionEngine executionEngine = Objects.requireNonNull(
                commandEngineDecorator.apply(commandEngine::execute),
                "commandEngineDecorator result"
        );
        commandGroup = new DefaultEventExecutorGroup(1);
        RedisReplyWriterFactory replyWriterFactory = new RespReplyWriterFactory();
        CommandExecutorConfig executorConfig = CommandExecutorConfigs.from(runtimeConfig);
        SerialOwnerExecutor commandOwner = new NettySerialOwnerExecutor(commandGroup.next());
        executor = new CommandExecutor<>(
                runtimeAccess::bindToCurrentThread,
                executionEngine,
                commandOwner,
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
                .childHandler(new YierdisServerChannelInitializer(
                        runtimeConfig,
                        executor,
                        replyWriterFactory,
                        inboundMemoryBudget,
                        outboundMemoryBudget,
                        childChannelRegistry,
                        replyEgressStats
                ));

        serverChannel = bootstrap.bind(runtimeConfig.bind(), runtimeConfig.port()).sync().channel();
        synchronized (lifecycleLock) {
            lifecycleState = LifecycleState.RUNNING;
        }
    }

    private static void configureDefaultDbEngineFactory(
            YierdisInstanceConfig.Builder instanceConfig,
            YierdisServerRuntimeConfig runtimeConfig
    ) {
        StableMemoryBackendFactory backendFactory = YierdisFfmStableMemoryBackend::new;
        DbEngineFactory engineFactory = new YierdisDbEngineFactory(
                backendFactory,
                new yier.bubu.redis.storage.memory.YierdisDbBackendConfig(
                        runtimeConfig.nativeSlotCapacity()
                )
        );
        instanceConfig.engineFactory(engineFactory);
    }

    @Override
    public void close() {
        CompletableFuture<Void> attempt;
        boolean performClose = false;
        synchronized (lifecycleLock) {
            if (closeAttempt == null) {
                closeAttempt = new CompletableFuture<>();
                lifecycleState = LifecycleState.CLOSING;
                performClose = true;
            }
            attempt = closeAttempt;
        }
        if (performClose) {
            try {
                closeInternal();
                synchronized (lifecycleLock) {
                    lifecycleState = LifecycleState.CLOSED;
                }
                attempt.complete(null);
            } catch (Throwable failure) {
                synchronized (lifecycleLock) {
                    lifecycleState = LifecycleState.FAILED;
                }
                attempt.completeExceptionally(failure);
            }
        }
        awaitCloseAttempt(attempt);
    }

    LifecycleState lifecycleStateForTests() {
        return lifecycleState;
    }

    private void closeInternal() {
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

        ChildChannelRegistry children = childChannelRegistry;
        List<Channel> acceptedChildren = List.of();
        if (children != null) {
            try {
                acceptedChildren = children.beginShutdown();
                markChildrenClosing(acceptedChildren);
            } catch (Throwable t) {
                failure = recordCloseFailure(failure, t);
            }
        }

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

        ChildDrainResult childDrain = drainChildReplies(children, acceptedChildren, ex);
        failure = recordCloseFailure(failure, childDrain.failure());
        // 即使 transport/回复 drain 已无法确认完成，也必须继续关闭预算、DB 和线程组，避免一次失败把整个实例永久留在半关闭态。
        childChannelRegistry = null;

        InboundMemoryBudget inboundBudget = inboundMemoryBudget;
        if (inboundBudget != null) {
            try {
                inboundBudget.close();
            } catch (Throwable t) {
                failure = recordCloseFailure(failure, t);
            }
        }
        inboundMemoryBudget = null;

        OutboundMemoryBudget outboundBudget = outboundMemoryBudget;
        if (outboundBudget != null) {
            try {
                outboundBudget.close();
            } catch (Throwable t) {
                failure = recordCloseFailure(failure, t);
            }
        }
        outboundMemoryBudget = null;
        replyEgressStats = null;

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

    private static void awaitCloseAttempt(CompletableFuture<Void> attempt) {
        try {
            attempt.join();
        } catch (java.util.concurrent.CompletionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("close failed", cause == null ? failure : cause);
        }
    }

    NettyServerInfoProvider infoProviderForTests() {
        return infoProvider;
    }

    InboundMemoryBudget inboundMemoryBudgetForTests() {
        return inboundMemoryBudget;
    }

    OutboundMemoryBudget outboundMemoryBudgetForTests() {
        return outboundMemoryBudget;
    }

    ReplyEgressStats replyEgressStatsForTests() {
        return replyEgressStats;
    }

    ChildChannelRegistry childChannelRegistryForTests() {
        return childChannelRegistry;
    }

    private ChildDrainResult drainChildReplies(
            ChildChannelRegistry children,
            List<Channel> acceptedChildren,
            CommandExecutor<NettyExecutionConnection> commandExecutor
    ) {
        if (children == null) {
            return ChildDrainResult.success();
        }

        List<CompletableFuture<Void>> replyDrains = new ArrayList<>(acceptedChildren.size());
        for (Channel child : acceptedChildren) {
            try {
                NettyExecutionConnection connection = NettyExecutionConnection.get(child);
                replyDrains.add(connection == null
                        ? closeUninitializedChild(child)
                        : connection.shutdownReplyGracefully());
            } catch (Throwable schedulingFailure) {
                // event loop 已退出时 shutdownReplyGracefully 可能同步拒绝；把失败纳入聚合，后续仍会 force-close 并回收其余资源。
                replyDrains.add(CompletableFuture.failedFuture(schedulingFailure));
            }
        }
        CompletableFuture<Void> replies = CompletableFuture.allOf(replyDrains.toArray(CompletableFuture[]::new));
        CompletableFuture<Void> allChildren = CompletableFuture.allOf(replies, children.drainedFuture());
        try {
            awaitDrain(allChildren);
            flushReplyCleanupTasks(commandExecutor);
            return ChildDrainResult.success();
        } catch (TimeoutException timeout) {
            IllegalStateException timeoutFailure = replyDrainTimeout(children, timeout);
            children.forceClose();
            try {
                awaitDrain(children.drainedFuture());
                flushReplyCleanupTasks(commandExecutor);
                return ChildDrainResult.drainedWithFailure(timeoutFailure);
            } catch (Throwable forcedCloseFailure) {
                timeoutFailure.addSuppressed(forcedCloseFailure);
                return ChildDrainResult.notDrained(timeoutFailure);
            }
        } catch (Throwable drainFailure) {
            children.forceClose();
            try {
                awaitDrain(children.drainedFuture());
                flushReplyCleanupTasks(commandExecutor);
            } catch (Throwable forcedCloseFailure) {
                drainFailure.addSuppressed(forcedCloseFailure);
                return ChildDrainResult.notDrained(drainFailure);
            }
            return ChildDrainResult.drainedWithFailure(drainFailure);
        }
    }

    private void awaitDrain(CompletableFuture<Void> future)
            throws InterruptedException, ExecutionException, TimeoutException {
        future.get(runtimeConfig.replyDrainTimeoutMillis(), TimeUnit.MILLISECONDS);
    }

    private static void markChildrenClosing(List<Channel> children) {
        for (Channel child : children) {
            NettyExecutionConnection connection = NettyExecutionConnection.get(child);
            if (connection != null) {
                connection.markClosing();
            }
        }
    }

    private static CompletableFuture<Void> closeUninitializedChild(Channel child) {
        CompletableFuture<Void> closed = new CompletableFuture<>();
        child.closeFuture().addListener(future -> {
            if (future.isSuccess()) {
                closed.complete(null);
            } else {
                closed.completeExceptionally(future.cause());
            }
        });
        try {
            if (child.isRegistered()) {
                child.close();
            } else {
                child.unsafe().closeForcibly();
            }
        } catch (Throwable closeFailure) {
            closed.completeExceptionally(closeFailure);
        }
        return closed;
    }

    private void flushReplyCleanupTasks(CommandExecutor<NettyExecutionConnection> commandExecutor) {
        if (commandExecutor != null) {
            commandExecutor.executeOwnerTask(() -> { }).join();
        }
    }

    private IllegalStateException replyDrainTimeout(ChildChannelRegistry children, TimeoutException cause) {
        ReplyEgressStats statsCollector = replyEgressStats;
        if (statsCollector != null) {
            statsCollector.shutdownTimeout();
        }
        OutboundMemoryBudgetStats stats = outboundMemoryBudget == null
                ? null
                : outboundMemoryBudget.stats();
        String diagnostics = stats == null
                ? "liveChildren=" + children.activeChannelCount()
                : "liveChildren=" + children.activeChannelCount()
                + ", reservedBytes=" + stats.reservedBytes()
                + ", allocatedBytes=" + stats.allocatedBytes()
                + ", activeConnections=" + stats.activeConnections()
                + ", activeSlots=" + stats.activeSlots();
        return new IllegalStateException(
                "reply shutdown timed out after " + runtimeConfig.replyDrainTimeoutMillis() + " ms: " + diagnostics,
                cause
        );
    }

    private record ChildDrainResult(boolean drained, Throwable failure) {
        private static ChildDrainResult success() {
            return new ChildDrainResult(true, null);
        }

        private static ChildDrainResult drainedWithFailure(Throwable failure) {
            return new ChildDrainResult(true, failure);
        }

        private static ChildDrainResult notDrained(Throwable failure) {
            return new ChildDrainResult(false, failure);
        }
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
