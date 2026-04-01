package yier.bubu.redis;

// Netty 单线程命令执行器：I/O 线程入队，执行器线程串行执行并写回响应；包含有界 backlog、背压与公平调度等机制。

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.EventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.contract.Command;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.contract.ReplyWriterFactory;
import yier.bubu.redis.executor.ExecutorBacklogBudget;
import yier.bubu.redis.executor.ExecutorBackpressureController;
import yier.bubu.redis.executor.ExecutorBackpressureIo;
import yier.bubu.redis.executor.ExecutorBackpressureObserver;
import yier.bubu.redis.executor.ExecutorBackpressureRuntime;
import yier.bubu.redis.executor.ExecutorTaskQueue;
import yier.bubu.redis.executor.SchedulingPolicy;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Netty-friendly single-thread command executor.
 * <p>
 * Commands are enqueued from I/O threads and executed on a dedicated Netty {@link EventExecutor} (single thread).
 * This keeps Redis-style "single thread command semantics", while providing bounded backlog, backpressure and
 * cooperative scheduling.
 * <p>
 * Key behaviors (contract):
 * <ul>
 *     <li><b>Bounded queue</b>: {@code queueCapacity} is a hard cap for the global backlog.
 *     When the queue is full (or executor is closing), {@link #trySubmit(ChannelHandlerContext, Command)}
 *     returns {@code false} and the caller is expected to fail-fast (server returns {@code -ERR busy}).</li>
 *     <li><b>Ownership</b>: on success, the executor takes ownership of {@link Command} and will recycle it.
 *     On failure, the caller must recycle.</li>
 *     <li><b>Connection-level backpressure</b>: backpressure is tracked per-channel by a pending counter.
 *     When {@code pending >= backpressureHighWatermark}, the executor disables Netty {@code autoRead} for that
 *     channel (enter backpressure). When {@code pending <= backpressureLowWatermark}, it re-enables {@code autoRead}
 *     (exit backpressure). This high/low hysteresis avoids oscillation.</li>
 *     <li><b>Drain budget (cooperative)</b>: a drain "tick" stops when either:
 *       <ul>
 *         <li>{@code processed >= maxDrainCommands}, or</li>
 *         <li>{@code now >= start + drainTimeLimitMillis}</li>
 *       </ul>
 *       The time limit is a <b>budget</b>, not a {@code sleep}. When the budget is hit and the queue is still not
 *       empty, the executor schedules the next drain tick, allowing other tasks on the same executor (e.g. scheduled
 *       TTL cleanup) to run between ticks.</li>
 *     <li><b>Flush coalescing</b>: commands write replies via {@link yier.bubu.redis.contract.ReplyWriter} into Netty buffers; each tick batches
 *     {@code write(...)} calls and performs a single {@code flush()} per channel at the end.</li>
 * </ul>
 * <p>
 * Configuration SSOT: these values are parsed and validated in {@code yierdis-args} ({@code YierdisServerArgs}).
 * Relevant flags:
 * {@code --executorQueueCapacity}, {@code --executorMaxDrain}, {@code --executorDrainMillis},
 * {@code --backpressureHigh}, {@code --backpressureLow}.
 */
public final class NettyCommandExecutor implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(NettyCommandExecutor.class);

    // 连接态：ServerSessionState（SELECT/AUTH/MULTI...）与 ServerRuntimeState（pending/backpressure/counters/closing）分离绑定。
    // 执行器调度状态在 NettyExecutorChannelState（避免跨层耦合/避免把调度细节放入协议层 Session）。
    // 注：Custom Protocol v1 的请求解码为 heap 命令（无 ByteBuf slice 驻留），无需 frame compaction。

    private final Runnable bindToCurrentThread;
    private final EventExecutor executor;
    private final SchedulingPolicy schedulingPolicy;

    // GLOBAL scheduling uses a bounded FIFO queue (legacy behavior).
    private final ArrayBlockingQueue<NettyExecutorTask> queue;
    private final ExecutorTaskQueue<Channel, NettyExecutorTask> taskQueue;
    private final ExecutorBacklogBudget backlogBudget;

    // --- Observability (hot-path friendly counters) ---
    private final LongAdder submitAccepted = new LongAdder();
    private final LongAdder submitRejectedNotRunning = new LongAdder();
    private final LongAdder submitRejectedQueueFull = new LongAdder();
    private final LongAdder submitRejectedBytesBudget = new LongAdder();
    private final LongAdder submitRejectedOfferFailed = new LongAdder();
    private final LongAdder commandsExecuted = new LongAdder();
    private final LongAdder commandsSkippedClosing = new LongAdder();
    private final LongAdder closeAfterReply = new LongAdder();
    private final LongAdder backpressureEnter = new LongAdder();
    private final LongAdder backpressureExit = new LongAdder();
    private final LongAdder drainLimitedByMaxCommands = new LongAdder();
    private final LongAdder drainLimitedByTimeBudget = new LongAdder();

    private final int backpressureHighWatermark;
    private final int backpressureLowWatermark;
    private final long backpressureBytesHighWatermark;
    private final long backpressureBytesLowWatermark;
    private final int maxDrainCommands;
    private final long drainTimeLimitNanos;

    private volatile boolean running = true;
    private final ExecutorBackpressureController<Channel> backpressureController;
    private final NettyCommandExecutionSupport executionSupport;
    private final NettyCommandSubmitter submitter;
    private final NettyCommandDrainLoop drainLoop;

    public NettyCommandExecutor(
            Runnable bindToCurrentThread,
            YierdisFastCommandProcessor commandProcessor,
            EventExecutor executor,
            ReplyWriterFactory replyWriterFactory,
            NettyCommandExecutorConfig config
    ) {
        this(
                bindToCurrentThread,
                commandProcessor,
                executor,
                replyWriterFactory,
                Objects.requireNonNull(config, "config").queueCapacity(),
                config.queueMaxBytes(),
                config.backpressureHighWatermark(),
                config.backpressureLowWatermark(),
                config.backpressureBytesHighWatermark(),
                config.backpressureBytesLowWatermark(),
                config.maxDrainCommands(),
                config.drainTimeLimitMillis(),
                config.schedulingPolicy()
        );
    }

    public NettyCommandExecutor(
            Runnable bindToCurrentThread,
            YierdisFastCommandProcessor commandProcessor,
            EventExecutor executor,
            ReplyWriterFactory replyWriterFactory,
            int queueCapacity,
            long queueMaxBytes,
            int backpressureHighWatermark,
            int backpressureLowWatermark,
            long backpressureBytesHighWatermark,
            long backpressureBytesLowWatermark,
            int maxDrainCommands,
            long drainTimeLimitMillis,
            SchedulingPolicy schedulingPolicy
    ) {
        this.bindToCurrentThread = Objects.requireNonNull(bindToCurrentThread, "bindToCurrentThread");
        Objects.requireNonNull(commandProcessor, "commandProcessor");
        this.executor = Objects.requireNonNull(executor, "executor");
        Objects.requireNonNull(replyWriterFactory, "replyWriterFactory");
        this.schedulingPolicy = schedulingPolicy == null ? SchedulingPolicy.FAIR : schedulingPolicy;
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be > 0");
        }
        if (queueMaxBytes < 0) {
            throw new IllegalArgumentException("queueMaxBytes must be >= 0");
        }
        if (backpressureHighWatermark <= 0) {
            throw new IllegalArgumentException("backpressureHighWatermark must be > 0");
        }
        if (backpressureLowWatermark < 0) {
            throw new IllegalArgumentException("backpressureLowWatermark must be >= 0");
        }
        if (backpressureLowWatermark >= backpressureHighWatermark) {
            throw new IllegalArgumentException("backpressureLowWatermark must be < backpressureHighWatermark");
        }
        if (backpressureBytesHighWatermark < 0) {
            throw new IllegalArgumentException("backpressureBytesHighWatermark must be >= 0");
        }
        if (backpressureBytesLowWatermark < 0) {
            throw new IllegalArgumentException("backpressureBytesLowWatermark must be >= 0");
        }
        if (backpressureBytesHighWatermark == 0 && backpressureBytesLowWatermark != 0) {
            throw new IllegalArgumentException("backpressureBytesLowWatermark must be 0 when backpressureBytesHighWatermark is 0");
        }
        if (backpressureBytesHighWatermark > 0 && backpressureBytesLowWatermark >= backpressureBytesHighWatermark) {
            throw new IllegalArgumentException("backpressureBytesLowWatermark must be < backpressureBytesHighWatermark");
        }
        if (maxDrainCommands <= 0) {
            throw new IllegalArgumentException("maxDrainCommands must be > 0");
        }
        if (drainTimeLimitMillis <= 0) {
            throw new IllegalArgumentException("drainTimeLimitMillis must be > 0");
        }

        this.queue = this.schedulingPolicy == SchedulingPolicy.GLOBAL ? new ArrayBlockingQueue<>(queueCapacity) : null;
        this.taskQueue = new ExecutorTaskQueue<>(
                this.schedulingPolicy,
                this.queue,
                channel -> ServerConnectionContext.getOrCreate(channel).scheduling()
        );
        this.backlogBudget = new ExecutorBacklogBudget(queueCapacity, queueMaxBytes);
        this.backpressureHighWatermark = backpressureHighWatermark;
        this.backpressureLowWatermark = backpressureLowWatermark;
        this.backpressureBytesHighWatermark = backpressureBytesHighWatermark;
        this.backpressureBytesLowWatermark = backpressureBytesLowWatermark;
        this.maxDrainCommands = maxDrainCommands;
        this.drainTimeLimitNanos = TimeUnit.MILLISECONDS.toNanos(drainTimeLimitMillis);

        ExecutorBackpressureIo<Channel> io = new ExecutorBackpressureIo<>() {
            @Override
            public boolean isActive(Channel key) {
                return key != null && key.isActive();
            }

            @Override
            public boolean isWritable(Channel key) {
                return key != null && key.isWritable();
            }

            @Override
            public void disableAutoRead(Channel key) {
                if (key == null) {
                    return;
                }
                key.eventLoop().execute(() -> {
                    try {
                        key.config().setAutoRead(false);
                    } catch (Throwable ignored) {
                        // ignore
                    }
                });
            }

            @Override
            public void enableAutoRead(Channel key) {
                if (key == null) {
                    return;
                }
                key.eventLoop().execute(() -> {
                    try {
                        key.config().setAutoRead(true);
                    } catch (Throwable ignored) {
                        // ignore
                    }
                });
            }

            @Override
            public void onClose(Channel key, Runnable callback) {
                if (key == null || callback == null) {
                    return;
                }
                key.closeFuture().addListener(ignored -> {
                    try {
                        callback.run();
                    } catch (Throwable ignored2) {
                        // ignore
                    }
                });
            }
        };

        ExecutorBackpressureRuntime<Channel> runtime = new ExecutorBackpressureRuntime<>() {
            @Override
            public int pending(Channel key) {
                return ServerConnectionContext.getOrCreate(key).runtime().pendingCounter().get();
            }

            @Override
            public long pendingBytes(Channel key) {
                return ServerConnectionContext.getOrCreate(key).runtime().pendingBytesCounter().get();
            }

            @Override
            public boolean isClosing(Channel key) {
                return ServerConnectionContext.getOrCreate(key).runtime().isClosing();
            }

            @Override
            public boolean markAutoReadDisabledByExecutor(Channel key) {
                return ServerConnectionContext.getOrCreate(key).runtime().markAutoReadDisabledByExecutor();
            }

            @Override
            public boolean autoReadDisabledByExecutor(Channel key) {
                return ServerConnectionContext.getOrCreate(key).runtime().autoReadDisabledByExecutor();
            }

            @Override
            public boolean clearAutoReadDisabledByExecutor(Channel key) {
                return ServerConnectionContext.getOrCreate(key).runtime().clearAutoReadDisabledByExecutor();
            }
        };

        ExecutorBackpressureObserver<Channel> observer = new ExecutorBackpressureObserver<>() {
            @Override
            public void onEnter(Channel key) {
                if (key == null) {
                    return;
                }
                try {
                    ServerConnectionContext.getOrCreate(key).runtime().backpressureEnterCounter().incrementAndGet();
                } catch (Throwable ignored) {
                    // ignore
                }
                backpressureEnter.increment();
            }

            @Override
            public void onExit(Channel key) {
                if (key == null) {
                    return;
                }
                try {
                    ServerConnectionContext.getOrCreate(key).runtime().backpressureExitCounter().incrementAndGet();
                } catch (Throwable ignored) {
                    // ignore
                }
                backpressureExit.increment();
            }
        };

        this.backpressureController = new ExecutorBackpressureController<>(
                this.executor,
                this.backlogBudget,
                this.backpressureLowWatermark,
                this.backpressureBytesHighWatermark,
                this.backpressureBytesLowWatermark,
                io,
                runtime,
                observer,
                () -> this.running
        );

        this.executionSupport = new NettyCommandExecutionSupport(
                log,
                commandProcessor,
                replyWriterFactory,
                this.backlogBudget,
                this.backpressureController,
                this.backpressureLowWatermark,
                this.backpressureBytesHighWatermark,
                this.backpressureBytesLowWatermark,
                () -> this.running,
                this.commandsExecuted,
                this.commandsSkippedClosing,
                this.closeAfterReply
        );
        this.drainLoop = new NettyCommandDrainLoop(
                this.executor,
                this.taskQueue,
                this.executionSupport,
                this.maxDrainCommands,
                this.drainTimeLimitNanos,
                () -> this.running,
                this.drainLimitedByMaxCommands,
                this.drainLimitedByTimeBudget
        );
        this.submitter = new NettyCommandSubmitter(
                log,
                this.backlogBudget,
                this.taskQueue,
                this.backpressureController,
                this.backpressureHighWatermark,
                this.backpressureBytesHighWatermark,
                () -> this.running,
                this.drainLoop::scheduleDrain,
                this.submitAccepted,
                this.submitRejectedNotRunning,
                this.submitRejectedQueueFull,
                this.submitRejectedBytesBudget,
                this.submitRejectedOfferFailed
        );
    }

    ReplyWriter newReplyWriter(ByteBuf out, Channel ch) {
        return executionSupport.newReplyWriter(out, ch);
    }

    public EventExecutor executor() {
        return executor;
    }

    /**
     * Disables Netty autoRead for the given channel using the same mechanism as executor backlog backpressure.
     * <p>
     * This is used by I/O handlers (e.g. writability changed) to stop reading when the outbound buffer is full.
     */
    void disableAutoRead(Channel ch) {
        backpressureController.disableAutoRead(ch);
    }

    /**
     * Called when a channel becomes writable again (outbound buffer drained below low watermark).
     * <p>
     * This triggers a best-effort re-check of backpressure conditions and may re-enable autoRead if appropriate.
     */
    void onChannelWritable(Channel ch) {
        if (ch == null) {
            return;
        }
        if (!running) {
            return;
        }
        if (executor.inEventLoop()) {
            maybeEnableAutoReadAfterWritable(ch);
            return;
        }
        executor.execute(() -> maybeEnableAutoReadAfterWritable(ch));
    }

    private void maybeEnableAutoReadAfterWritable(Channel ch) {
        if (ch == null) {
            return;
        }
        if (!running) {
            return;
        }
        if (!ch.isActive()) {
            return;
        }
        if (NettyCommandExecutionSupport.isChannelClosing(ch)) {
            return;
        }
        if (!ch.isWritable()) {
            // Still not writable; keep autoRead disabled.
            return;
        }

        ServerRuntimeState conn = ServerConnectionContext.getOrCreate(ch).runtime();
        int pending = conn.pendingCounter().get();
        long pendingBytes = conn.pendingBytesCounter().get();

        boolean pendingOk = pending <= backpressureLowWatermark;
        boolean bytesOk = backpressureBytesHighWatermark <= 0 || pendingBytes <= backpressureBytesLowWatermark;
        boolean globalOk = backlogBudget.isGlobalBackpressureCleared();

        if (pendingOk && bytesOk && globalOk) {
            backpressureController.enableAutoReadIfWeDisabled(ch);
        }
        if (globalOk) {
            backpressureController.scheduleGlobalRecovery();
        }
    }

    /**
     * Binds the DB to the executor thread (single-thread semantics).
     */
    public void start() {
        executor.submit(bindToCurrentThread).syncUninterruptibly();
        drainLoop.markStarted();
        drainLoop.scheduleDrain();
    }

    /**
     * Attempts to submit a client command for execution.
     * <p>
     * Success: the executor takes ownership of {@link Command} and is responsible for close().
     * Failure: the caller retains ownership and MUST recycle().
     */
    public boolean trySubmit(ChannelHandlerContext ctx, Command cmd) {
        return trySubmitWithReason(ctx, cmd) == null;
    }

    enum SubmitRejectReason {
        NOT_RUNNING("not_running"),
        QUEUE_FULL("queue_full"),
        BYTES_BUDGET("bytes_budget"),
        OFFER_FAILED("offer_failed");

        private final String code;

        SubmitRejectReason(String code) {
            this.code = code == null ? "unknown" : code;
        }

        String code() {
            return code;
        }
    }

    SubmitRejectReason trySubmitWithReason(ChannelHandlerContext ctx, Command cmd) {
        return submitter.trySubmitWithReason(ctx, cmd);
    }

    public void executeMaintenance(Runnable task) {
        Objects.requireNonNull(task, "task");
        if (!running) {
            return;
        }
        executor.execute(() -> {
            try {
                task.run();
            } catch (Throwable t) {
                log.debug("Maintenance task failed", t);
            }
        });
    }

    @Override
    public void close() {
        running = false;
        drainLoop.scheduleDrain();
    }

    /**
     * Stops accepting new commands and drains/recycles any pending queued commands.
     * <p>
     * The returned future completes after the executor thread has processed the drain barrier, meaning it is safe
     * to run single-threaded teardown work on the same executor afterwards (e.g. DB shutdown).
     */
    public io.netty.util.concurrent.Future<?> shutdownGracefully() {
        running = false;
        drainLoop.scheduleDrain();
        return executor.submit(drainLoop::drainLeftoverCommands);
    }

    public static final class StatsSnapshot {
        public final int queuedTasks;
        public final long queuedBytes;
        public final int queueCapacity;
        public final long queueMaxBytes;
        public final SchedulingPolicy schedulingPolicy;
        public final int backpressureHighWatermark;
        public final int backpressureLowWatermark;
        public final long backpressureBytesHighWatermark;
        public final long backpressureBytesLowWatermark;
        public final int globalBackpressureHighWatermark;
        public final int globalBackpressureLowWatermark;
        public final long globalBackpressureBytesHighWatermark;
        public final long globalBackpressureBytesLowWatermark;
        public final int maxDrainCommands;
        public final long drainTimeLimitNanos;
        public final int channelsAutoReadDisabled;
        public final long submitAccepted;
        public final long submitRejectedNotRunning;
        public final long submitRejectedQueueFull;
        public final long submitRejectedBytesBudget;
        public final long submitRejectedOfferFailed;
        public final long commandsExecuted;
        public final long commandsSkippedClosing;
        public final long closeAfterReply;
        public final long backpressureEnter;
        public final long backpressureExit;
        public final long drainLimitedByMaxCommands;
        public final long drainLimitedByTimeBudget;

        private StatsSnapshot(
                int queuedTasks,
                long queuedBytes,
                int queueCapacity,
                long queueMaxBytes,
                SchedulingPolicy schedulingPolicy,
                int backpressureHighWatermark,
                int backpressureLowWatermark,
                long backpressureBytesHighWatermark,
                long backpressureBytesLowWatermark,
                int globalBackpressureHighWatermark,
                int globalBackpressureLowWatermark,
                long globalBackpressureBytesHighWatermark,
                long globalBackpressureBytesLowWatermark,
                int maxDrainCommands,
                long drainTimeLimitNanos,
                int channelsAutoReadDisabled,
                long submitAccepted,
                long submitRejectedNotRunning,
                long submitRejectedQueueFull,
                long submitRejectedBytesBudget,
                long submitRejectedOfferFailed,
                long commandsExecuted,
                long commandsSkippedClosing,
                long closeAfterReply,
                long backpressureEnter,
                long backpressureExit,
                long drainLimitedByMaxCommands,
                long drainLimitedByTimeBudget
        ) {
            this.queuedTasks = queuedTasks;
            this.queuedBytes = queuedBytes;
            this.queueCapacity = queueCapacity;
            this.queueMaxBytes = queueMaxBytes;
            this.schedulingPolicy = schedulingPolicy;
            this.backpressureHighWatermark = backpressureHighWatermark;
            this.backpressureLowWatermark = backpressureLowWatermark;
            this.backpressureBytesHighWatermark = backpressureBytesHighWatermark;
            this.backpressureBytesLowWatermark = backpressureBytesLowWatermark;
            this.globalBackpressureHighWatermark = globalBackpressureHighWatermark;
            this.globalBackpressureLowWatermark = globalBackpressureLowWatermark;
            this.globalBackpressureBytesHighWatermark = globalBackpressureBytesHighWatermark;
            this.globalBackpressureBytesLowWatermark = globalBackpressureBytesLowWatermark;
            this.maxDrainCommands = maxDrainCommands;
            this.drainTimeLimitNanos = drainTimeLimitNanos;
            this.channelsAutoReadDisabled = channelsAutoReadDisabled;
            this.submitAccepted = submitAccepted;
            this.submitRejectedNotRunning = submitRejectedNotRunning;
            this.submitRejectedQueueFull = submitRejectedQueueFull;
            this.submitRejectedBytesBudget = submitRejectedBytesBudget;
            this.submitRejectedOfferFailed = submitRejectedOfferFailed;
            this.commandsExecuted = commandsExecuted;
            this.commandsSkippedClosing = commandsSkippedClosing;
            this.closeAfterReply = closeAfterReply;
            this.backpressureEnter = backpressureEnter;
            this.backpressureExit = backpressureExit;
            this.drainLimitedByMaxCommands = drainLimitedByMaxCommands;
            this.drainLimitedByTimeBudget = drainLimitedByTimeBudget;
        }
    }

    public StatsSnapshot statsSnapshot() {
        return new StatsSnapshot(
                backlogBudget.queuedTasks(),
                backlogBudget.queuedBytes(),
                backlogBudget.queueCapacity(),
                backlogBudget.queueMaxBytes(),
                schedulingPolicy,
                backpressureHighWatermark,
                backpressureLowWatermark,
                backpressureBytesHighWatermark,
                backpressureBytesLowWatermark,
                backlogBudget.globalBackpressureHighWatermark(),
                backlogBudget.globalBackpressureLowWatermark(),
                backlogBudget.globalBackpressureBytesHighWatermark(),
                backlogBudget.globalBackpressureBytesLowWatermark(),
                maxDrainCommands,
                drainTimeLimitNanos,
                backpressureController.keysAutoReadDisabledCount(),
                submitAccepted.sum(),
                submitRejectedNotRunning.sum(),
                submitRejectedQueueFull.sum(),
                submitRejectedBytesBudget.sum(),
                submitRejectedOfferFailed.sum(),
                commandsExecuted.sum(),
                commandsSkippedClosing.sum(),
                closeAfterReply.sum(),
                backpressureEnter.sum(),
                backpressureExit.sum(),
                drainLimitedByMaxCommands.sum(),
                drainLimitedByTimeBudget.sum()
        );
    }

    static int safeRetainedBytes(Command cmd) {
        return NettyCommandExecutionSupport.safeRetainedBytes(cmd);
    }
}
