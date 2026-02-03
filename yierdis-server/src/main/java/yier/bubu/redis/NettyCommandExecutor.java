package yier.bubu.redis;

// Netty 单线程命令执行器：I/O 线程入队，执行器线程串行执行并写回响应；包含有界 backlog、背压与公平调度等机制。

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.EventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yier.bubu.redis.bytes.netty.NettyByteBufSink;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespWriter;

import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
 *     When the queue is full (or executor is closing), {@link #trySubmit(ChannelHandlerContext, RespCommand)}
 *     returns {@code false} and the caller is expected to fail-fast (server returns {@code -ERR busy}).</li>
 *     <li><b>Ownership</b>: on success, the executor takes ownership of {@link RespCommand} and will recycle it.
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
 *     <li><b>Flush coalescing</b>: commands write replies via {@link RespWriter} into Netty buffers; each tick batches
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

    public enum SchedulingPolicy {
        GLOBAL,
        FAIR
    }

    // 连接态：协议会话在 ConnectionContext（protocol-netty）；server 运行时连接态在 ServerConnectionState（server 私有）。
    // 执行器调度状态在 NettyExecutorChannelState（避免跨层耦合）。

    // Compaction is intentionally conservative: only compact "small frames that retain too much memory" to reduce
    // the risk of copying large payloads.
    private static final int DEFAULT_FRAME_COMPACTION_MAX_COPY_BYTES = 1024 * 1024; // 1 MiB

    private final Runnable bindToCurrentThread;
    private final YierdisFastCommandProcessor commandProcessor;
    private final EventExecutor executor;

    private final SchedulingPolicy schedulingPolicy;

    // GLOBAL scheduling uses a bounded FIFO queue (legacy behavior).
    private final ArrayBlockingQueue<NettyExecutorTask> queue;
    private final NettyExecutorTaskQueue taskQueue;
    private final NettyExecutorBacklogBudget backlogBudget;

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

    private final NettyExecutorFrameCompactor frameCompactor;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean drainScheduled = new AtomicBoolean(false);
    private volatile boolean running = true;
    private final NettyExecutorBackpressureController backpressureController;

    public NettyCommandExecutor(
            YierdisDb db,
            YierdisFastCommandProcessor commandProcessor,
            EventExecutor executor,
            int queueCapacity,
            long queueMaxBytes,
            int backpressureHighWatermark,
            int backpressureLowWatermark,
            long backpressureBytesHighWatermark,
            long backpressureBytesLowWatermark,
            int maxDrainCommands,
            long drainTimeLimitMillis
    ) {
        this(
                Objects.requireNonNull(db, "db")::bindToCurrentThread,
                commandProcessor,
                executor,
                queueCapacity,
                queueMaxBytes,
                backpressureHighWatermark,
                backpressureLowWatermark,
                backpressureBytesHighWatermark,
                backpressureBytesLowWatermark,
                maxDrainCommands,
                drainTimeLimitMillis,
                SchedulingPolicy.FAIR,
                0,
                2.0,
                DEFAULT_FRAME_COMPACTION_MAX_COPY_BYTES
        );
    }

    public NettyCommandExecutor(
            YierdisDb db,
            YierdisFastCommandProcessor commandProcessor,
            EventExecutor executor,
            int queueCapacity,
            long queueMaxBytes,
            int backpressureHighWatermark,
            int backpressureLowWatermark,
            long backpressureBytesHighWatermark,
            long backpressureBytesLowWatermark,
            int maxDrainCommands,
            long drainTimeLimitMillis,
            SchedulingPolicy schedulingPolicy,
            long frameCompactionThresholdBytes,
            double frameCompactionRatio,
            int frameCompactionMaxCopyBytes
    ) {
        this(
                Objects.requireNonNull(db, "db")::bindToCurrentThread,
                commandProcessor,
                executor,
                queueCapacity,
                queueMaxBytes,
                backpressureHighWatermark,
                backpressureLowWatermark,
                backpressureBytesHighWatermark,
                backpressureBytesLowWatermark,
                maxDrainCommands,
                drainTimeLimitMillis,
                schedulingPolicy,
                frameCompactionThresholdBytes,
                frameCompactionRatio,
                frameCompactionMaxCopyBytes
        );
    }

    public NettyCommandExecutor(
            Runnable bindToCurrentThread,
            YierdisFastCommandProcessor commandProcessor,
            EventExecutor executor,
            int queueCapacity,
            long queueMaxBytes,
            int backpressureHighWatermark,
            int backpressureLowWatermark,
            long backpressureBytesHighWatermark,
            long backpressureBytesLowWatermark,
            int maxDrainCommands,
            long drainTimeLimitMillis,
            SchedulingPolicy schedulingPolicy,
            long frameCompactionThresholdBytes,
            double frameCompactionRatio,
            int frameCompactionMaxCopyBytes
    ) {
        this.bindToCurrentThread = Objects.requireNonNull(bindToCurrentThread, "bindToCurrentThread");
        this.commandProcessor = Objects.requireNonNull(commandProcessor, "commandProcessor");
        this.executor = Objects.requireNonNull(executor, "executor");
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
        if (frameCompactionThresholdBytes < 0) {
            throw new IllegalArgumentException("frameCompactionThresholdBytes must be >= 0");
        }
        if (Double.isNaN(frameCompactionRatio) || frameCompactionRatio < 1.0) {
            throw new IllegalArgumentException("frameCompactionRatio must be >= 1.0");
        }
        if (frameCompactionMaxCopyBytes <= 0) {
            throw new IllegalArgumentException("frameCompactionMaxCopyBytes must be > 0");
        }

        this.queue = this.schedulingPolicy == SchedulingPolicy.GLOBAL ? new ArrayBlockingQueue<>(queueCapacity) : null;
        this.taskQueue = new NettyExecutorTaskQueue(this.schedulingPolicy, this.queue);
        this.backlogBudget = new NettyExecutorBacklogBudget(queueCapacity, queueMaxBytes);
        this.backpressureHighWatermark = backpressureHighWatermark;
        this.backpressureLowWatermark = backpressureLowWatermark;
        this.backpressureBytesHighWatermark = backpressureBytesHighWatermark;
        this.backpressureBytesLowWatermark = backpressureBytesLowWatermark;
        this.maxDrainCommands = maxDrainCommands;
        this.drainTimeLimitNanos = TimeUnit.MILLISECONDS.toNanos(drainTimeLimitMillis);

        this.frameCompactor = new NettyExecutorFrameCompactor(
                frameCompactionThresholdBytes,
                frameCompactionRatio,
                frameCompactionMaxCopyBytes
        );

        this.backpressureController = new NettyExecutorBackpressureController(
                this.executor,
                this.backlogBudget,
                this.backpressureLowWatermark,
                this.backpressureBytesHighWatermark,
                this.backpressureBytesLowWatermark,
                this.backpressureEnter,
                this.backpressureExit,
                () -> this.running
        );
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
        if (isChannelClosing(ch)) {
            return;
        }
        if (!ch.isWritable()) {
            // Still not writable; keep autoRead disabled.
            return;
        }

        ServerConnectionState conn = ServerConnectionState.getOrCreate(ch);
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
        started.set(true);
        scheduleDrain();
    }

    /**
     * Attempts to submit a client command for execution.
     * <p>
     * Success: the executor takes ownership of {@link RespCommand} and is responsible for recycle().
     * Failure: the caller retains ownership and MUST recycle().
     */
    public boolean trySubmit(ChannelHandlerContext ctx, RespCommand cmd) {
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

    SubmitRejectReason trySubmitWithReason(ChannelHandlerContext ctx, RespCommand cmd) {
        Objects.requireNonNull(ctx, "ctx");
        Objects.requireNonNull(cmd, "cmd");
        Channel ch = ctx.channel();
        ServerConnectionState conn = ServerConnectionState.getOrCreate(ch);
        if (!running) {
            submitRejectedNotRunning.increment();
            conn.commandsRejectedCounter().incrementAndGet();
            return SubmitRejectReason.NOT_RUNNING;
        }

        AtomicInteger pending = conn.pendingCounter();
        AtomicLong pendingBytes = conn.pendingBytesCounter();
        if (backlogBudget.isGlobalBackpressureHigh()) {
            backpressureController.disableAutoRead(ch);
        }
        if (pending.get() >= backpressureHighWatermark) {
            backpressureController.disableAutoRead(ch);
        }
        if (backpressureBytesHighWatermark > 0 && pendingBytes.get() >= backpressureBytesHighWatermark) {
            backpressureController.disableAutoRead(ch);
        }

        boolean reservedSlot = false;
        if (!backlogBudget.tryReserveSlot()) {
            // Global queue full: apply backpressure to avoid busy storms.
            backpressureController.disableAutoRead(ch);
            submitRejectedQueueFull.increment();
            conn.commandsRejectedCounter().incrementAndGet();
            return SubmitRejectReason.QUEUE_FULL;
        }
        reservedSlot = true;

        int retainedBytes = 0;
        boolean reservedBytes = false;
        try {
            frameCompactor.tryCompact(ctx, cmd);

            retainedBytes = Math.max(0, safeRetainedBytes(cmd));
            if (!backlogBudget.tryReserveQueuedBytes(retainedBytes)) {
                if (reservedSlot) {
                    backlogBudget.releaseSlot();
                }
                backpressureController.disableAutoRead(ch);
                submitRejectedBytesBudget.increment();
                conn.commandsRejectedCounter().incrementAndGet();
                return SubmitRejectReason.BYTES_BUDGET;
            }
            reservedBytes = true;

            boolean accepted = taskQueue.offer(ch, NettyExecutorTask.command(ctx, cmd, retainedBytes));
            if (!accepted) {
                backlogBudget.releaseQueuedBytes(retainedBytes);
                if (reservedSlot) {
                    backlogBudget.releaseSlot();
                }
                backpressureController.disableAutoRead(ch);
                submitRejectedOfferFailed.increment();
                conn.commandsRejectedCounter().incrementAndGet();
                return SubmitRejectReason.OFFER_FAILED;
            }

            submitAccepted.increment();
            conn.commandsEnqueuedCounter().incrementAndGet();
            int now = pending.incrementAndGet();
            if (now >= backpressureHighWatermark) {
                backpressureController.disableAutoRead(ch);
            }

            long bytesNow = pendingBytes.addAndGet(retainedBytes);
            if (backpressureBytesHighWatermark > 0 && bytesNow >= backpressureBytesHighWatermark) {
                backpressureController.disableAutoRead(ch);
            }
            if (backlogBudget.isGlobalBackpressureHigh()) {
                backpressureController.disableAutoRead(ch);
            }

            scheduleDrain();
            return null;
        } catch (Throwable t) {
            if (reservedBytes) {
                backlogBudget.releaseQueuedBytes(retainedBytes);
            }
            if (reservedSlot) {
                backlogBudget.releaseSlot();
            }
            submitRejectedOfferFailed.increment();
            conn.commandsRejectedCounter().incrementAndGet();
            log.debug("Failed to submit command", t);
            return SubmitRejectReason.OFFER_FAILED;
        }
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

    private void scheduleDrain() {
        if (running && !started.get()) {
            return;
        }
        if (!drainScheduled.compareAndSet(false, true)) {
            return;
        }
        executor.execute(this::drainLoop);
    }

    private void drainLoop() {
        if (!running) {
            drainScheduled.set(false);
            drainLeftoverCommands();
            return;
        }

        long deadline = System.nanoTime() + drainTimeLimitNanos;
        int processed = 0;
        boolean hitMaxDrainCommands = false;
        boolean hitDrainTimeBudget = false;

        IdentityHashMap<Channel, ChannelHandlerContext> flushTargets = new IdentityHashMap<>();
        for (; ; ) {
            if (processed >= maxDrainCommands) {
                hitMaxDrainCommands = true;
                break;
            }
            if (System.nanoTime() >= deadline) {
                hitDrainTimeBudget = true;
                break;
            }

            NettyExecutorTask task = taskQueue.poll();
            if (task == null) {
                break;
            }
            processed++;

            executeOne(task, flushTargets);
        }

        for (ChannelHandlerContext ctx : flushTargets.values()) {
            safeFlush(ctx);
        }

        boolean pendingAfterDrain = taskQueue.hasPendingTasks();
        if (pendingAfterDrain) {
            if (hitMaxDrainCommands) {
                drainLimitedByMaxCommands.increment();
            } else if (hitDrainTimeBudget) {
                drainLimitedByTimeBudget.increment();
            }
        }

        if (pendingAfterDrain) {
            executor.execute(this::drainLoop);
            return;
        }

        drainScheduled.set(false);
        if (taskQueue.hasPendingTasks() && drainScheduled.compareAndSet(false, true)) {
            executor.execute(this::drainLoop);
        }
    }

    private void executeOne(NettyExecutorTask task, IdentityHashMap<Channel, ChannelHandlerContext> flushTargets) {
        ChannelHandlerContext ctx = task.ctx;
        RespCommand cmd = task.cmd;
        if (ctx == null || cmd == null) {
            return;
        }
        Channel ch = ctx.channel();
        if (ch == null || !ch.isActive() || isChannelClosing(ch)) {
            // 连接已关闭或标记为 closing：只回收已入队的命令帧与预算，不再执行，避免产生副作用。
            ServerConnectionState conn = ServerConnectionState.getOrCreate(ch);
            conn.commandsSkippedClosingCounter().incrementAndGet();
            commandsSkippedClosing.increment();
            recycleAndRelease(task);
            return;
        }

        try {
            ByteBuf out = ctx.alloc().buffer();
            boolean ok = false;
            try {
                ServerConnectionState conn = ServerConnectionState.getOrCreate(ch);
                RespWriter writer = new RespWriter(new NettyByteBufSink(out), conn);
                commandProcessor.execute(cmd, writer);
                if (writer.closeAfterReplyRequested()) {
                    // close-after-reply：flush 后关闭连接，并标记该 channel 后续任务需要跳过。
                    conn.closeAfterReplyCounter().incrementAndGet();
                    closeAfterReply.increment();
                    conn.markClosing();
                    backpressureController.disableAutoRead(ctx.channel());
                    ctx.writeAndFlush(out).addListener(ChannelFutureListener.CLOSE);
                } else {
                    ctx.write(out, ctx.voidPromise());
                    flushTargets.put(ctx.channel(), ctx);
                }
                ok = true;
            } finally {
                if (!ok) {
                    out.release();
                }
            }
        } catch (Throwable t) {
            // Best-effort: try to return a generic error and keep the channel alive.
            try {
                ByteBuf out = ctx.alloc().buffer();
                boolean ok = false;
                try {
                    new RespWriter(new NettyByteBufSink(out), ServerConnectionState.getOrCreate(ch)).error("ERR internal error");
                    ctx.write(out, ctx.voidPromise());
                    flushTargets.put(ctx.channel(), ctx);
                    ok = true;
                } finally {
                    if (!ok) {
                        out.release();
                    }
                }
            } catch (Throwable ignored) {
                // ignore
            }
            log.debug("Command execution failed", t);
        } finally {
            try {
                cmd.close();
            } catch (Throwable ignored) {
                // ignore
            }
            onCommandFinished(ctx.channel(), task.retainedBytes, true);
        }
    }

    private void onCommandFinished(Channel ch, int retainedBytes, boolean executed) {
        ServerConnectionState conn = ServerConnectionState.getOrCreate(ch);
        if (executed) {
            conn.commandsExecutedCounter().incrementAndGet();
            commandsExecuted.increment();
        }

        AtomicInteger pending = conn.pendingCounter();
        int now = pending.decrementAndGet();
        AtomicLong pendingBytes = conn.pendingBytesCounter();
        long bytesNow = pendingBytes.addAndGet(-retainedBytes);
        backlogBudget.releaseQueuedBytes(retainedBytes);
        backlogBudget.releaseSlot();

        boolean pendingOk = now <= backpressureLowWatermark;
        boolean bytesOk = backpressureBytesHighWatermark <= 0 || bytesNow <= backpressureBytesLowWatermark;
        boolean globalOk = backlogBudget.isGlobalBackpressureCleared();
        if (running && !isChannelClosing(ch) && pendingOk && bytesOk && globalOk) {
            backpressureController.enableAutoReadIfWeDisabled(ch);
        }
        if (running && globalOk) {
            backpressureController.scheduleGlobalRecovery();
        }
    }

    private static void safeFlush(ChannelHandlerContext ctx) {
        if (ctx == null) {
            return;
        }
        try {
            ctx.flush();
        } catch (Throwable ignored) {
            // ignore
        }
    }

    private static boolean isChannelClosing(Channel ch) {
        if (ch == null) {
            return false;
        }
        return ServerConnectionState.getOrCreate(ch).isClosing();
    }

    private void drainLeftoverCommands() {
        taskQueue.drainLeftoverCommands(this::recycleAndRelease);
    }

    private void recycleAndRelease(NettyExecutorTask t) {
        if (t == null || t.cmd == null) {
            return;
        }
        try {
            t.cmd.close();
        } catch (Throwable ignored) {
            // ignore
        }
        if (t.ctx != null) {
            onCommandFinished(t.ctx.channel(), t.retainedBytes, false);
            return;
        }
        backlogBudget.releaseQueuedBytes(t.retainedBytes);
        backlogBudget.releaseSlot();
    }

    @Override
    public void close() {
        running = false;
        scheduleDrain();
    }

    /**
     * Stops accepting new commands and drains/recycles any pending queued commands.
     * <p>
     * The returned future completes after the executor thread has processed the drain barrier, meaning it is safe
     * to run single-threaded teardown work on the same executor afterwards (e.g. DB shutdown).
     */
    public io.netty.util.concurrent.Future<?> shutdownGracefully() {
        running = false;
        scheduleDrain();
        return executor.submit(this::drainLeftoverCommands);
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
                backpressureController.channelsAutoReadDisabledCount(),
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

    private static int safeRetainedBytes(RespCommand cmd) {
        if (cmd == null) {
            return 0;
        }
        try {
            if (cmd.frame() == null) {
                return 0;
            }
            return cmd.frame().retainedBytes();
        } catch (Throwable ignored) {
            return 0;
        }
    }

}
