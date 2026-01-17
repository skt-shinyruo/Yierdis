package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yier.bubu.redis.bytes.netty.NettyByteBufSink;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespCommandBuilder;
import yier.bubu.redis.protocol.RespFrame;
import yier.bubu.redis.protocol.RespWriter;
import yier.bubu.redis.protocol.netty.NettyRespFrame;
import yier.bubu.redis.protocol.netty.NettyRespSession;

import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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

    private static final AttributeKey<AtomicInteger> PENDING_PER_CHANNEL =
            AttributeKey.valueOf("yierdis.exec.pending");
    private static final AttributeKey<AtomicLong> PENDING_BYTES_PER_CHANNEL =
            AttributeKey.valueOf("yierdis.exec.pendingBytes");
    private static final AttributeKey<Boolean> AUTOREAD_DISABLED_BY_EXECUTOR =
            AttributeKey.valueOf("yierdis.exec.autoreadDisabled");
    private static final AttributeKey<ChannelState> CHANNEL_STATE =
            AttributeKey.valueOf("yierdis.exec.state");
    private static final AttributeKey<Boolean> CHANNEL_CLOSING =
            AttributeKey.valueOf("yierdis.exec.closing");

    // Compaction is intentionally conservative: only compact "small frames that retain too much memory" to reduce
    // the risk of copying large payloads.
    private static final int DEFAULT_FRAME_COMPACTION_MAX_COPY_BYTES = 1024 * 1024; // 1 MiB

    private final YierdisDb db;
    private final YierdisFastCommandProcessor commandProcessor;
    private final EventExecutor executor;

    private final SchedulingPolicy schedulingPolicy;

    // GLOBAL scheduling uses a bounded FIFO queue (legacy behavior).
    private final ArrayBlockingQueue<Task> queue;

    // FAIR scheduling uses per-channel queues + round-robin scheduling.
    private final ConcurrentLinkedQueue<Channel> activeChannels = new ConcurrentLinkedQueue<>();
    private final TaskQueue taskQueue;
    private final AtomicInteger queuedTasks = new AtomicInteger(0);
    private final AtomicLong queuedBytes = new AtomicLong(0);

    private final int queueCapacity;
    private final long queueMaxBytes;
    private final int backpressureHighWatermark;
    private final int backpressureLowWatermark;
    private final long backpressureBytesHighWatermark;
    private final long backpressureBytesLowWatermark;
    private final int globalBackpressureHighWatermark;
    private final int globalBackpressureLowWatermark;
    private final long globalBackpressureBytesHighWatermark;
    private final long globalBackpressureBytesLowWatermark;
    private final int maxDrainCommands;
    private final long drainTimeLimitNanos;

    private final long frameCompactionThresholdBytes;
    private final double frameCompactionRatio;
    private final int frameCompactionMaxCopyBytes;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean drainScheduled = new AtomicBoolean(false);
    private final AtomicBoolean globalRecoveryScheduled = new AtomicBoolean(false);
    private volatile boolean running = true;
    private final ConcurrentHashMap<Channel, Boolean> channelsWithAutoReadDisabled = new ConcurrentHashMap<>();

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
                db,
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
        this.db = Objects.requireNonNull(db, "db");
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

        this.queueCapacity = queueCapacity;
        this.queue = this.schedulingPolicy == SchedulingPolicy.GLOBAL ? new ArrayBlockingQueue<>(queueCapacity) : null;
        this.taskQueue = new TaskQueue();
        this.queueMaxBytes = queueMaxBytes;
        this.backpressureHighWatermark = backpressureHighWatermark;
        this.backpressureLowWatermark = backpressureLowWatermark;
        this.backpressureBytesHighWatermark = backpressureBytesHighWatermark;
        this.backpressureBytesLowWatermark = backpressureBytesLowWatermark;
        this.maxDrainCommands = maxDrainCommands;
        this.drainTimeLimitNanos = TimeUnit.MILLISECONDS.toNanos(drainTimeLimitMillis);

        this.frameCompactionThresholdBytes = frameCompactionThresholdBytes;
        this.frameCompactionRatio = frameCompactionRatio;
        this.frameCompactionMaxCopyBytes = frameCompactionMaxCopyBytes;

        int globalHigh = defaultGlobalBackpressureHighWatermark(queueCapacity);
        int globalLow = defaultGlobalBackpressureLowWatermark(globalHigh);
        long globalBytesHigh = defaultGlobalBackpressureBytesHighWatermark(queueMaxBytes);
        long globalBytesLow = defaultGlobalBackpressureBytesLowWatermark(globalBytesHigh);

        this.globalBackpressureHighWatermark = globalHigh;
        this.globalBackpressureLowWatermark = globalLow;
        this.globalBackpressureBytesHighWatermark = globalBytesHigh;
        this.globalBackpressureBytesLowWatermark = globalBytesLow;
    }

    public EventExecutor executor() {
        return executor;
    }

    /**
     * Binds the DB to the executor thread (single-thread semantics).
     */
    public void start() {
        executor.submit(db::bindToCurrentThread).syncUninterruptibly();
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
        Objects.requireNonNull(ctx, "ctx");
        Objects.requireNonNull(cmd, "cmd");
        if (!running) {
            return false;
        }

        Channel ch = ctx.channel();

        AtomicInteger pending = pendingCounter(ch);
        AtomicLong pendingBytes = pendingBytesCounter(ch);
        if (isGlobalBackpressureHigh()) {
            disableAutoRead(ch);
        }
        if (pending.get() >= backpressureHighWatermark) {
            disableAutoRead(ch);
        }
        if (backpressureBytesHighWatermark > 0 && pendingBytes.get() >= backpressureBytesHighWatermark) {
            disableAutoRead(ch);
        }

        boolean reservedSlot = false;
        if (!tryReserveQueueSlot()) {
            // Global queue full: apply backpressure to avoid busy storms.
            disableAutoRead(ch);
            return false;
        }
        reservedSlot = true;

        int retainedBytes = 0;
        boolean reservedBytes = false;
        try {
            tryCompactFrameIfNeeded(ctx, cmd);

            retainedBytes = Math.max(0, safeRetainedBytes(cmd));
            if (!tryReserveQueuedBytes(retainedBytes)) {
                if (reservedSlot) {
                    releaseQueueSlot();
                }
                disableAutoRead(ch);
                return false;
            }
            reservedBytes = true;

            boolean accepted = taskQueue.offer(ch, Task.command(ctx, cmd, retainedBytes));
            if (!accepted) {
                releaseQueuedBytes(retainedBytes);
                if (reservedSlot) {
                    releaseQueueSlot();
                }
                disableAutoRead(ch);
                return false;
            }

            int now = pending.incrementAndGet();
            if (now >= backpressureHighWatermark) {
                disableAutoRead(ch);
            }

            long bytesNow = pendingBytes.addAndGet(retainedBytes);
            if (backpressureBytesHighWatermark > 0 && bytesNow >= backpressureBytesHighWatermark) {
                disableAutoRead(ch);
            }
            if (isGlobalBackpressureHigh()) {
                disableAutoRead(ch);
            }

            scheduleDrain();
            return accepted;
        } catch (Throwable t) {
            if (reservedBytes) {
                releaseQueuedBytes(retainedBytes);
            }
            if (reservedSlot) {
                releaseQueueSlot();
            }
            log.debug("Failed to submit command", t);
            return false;
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

    private boolean tryReserveQueueSlot() {
        for (; ; ) {
            int cur = queuedTasks.get();
            if (cur >= queueCapacity) {
                return false;
            }
            if (queuedTasks.compareAndSet(cur, cur + 1)) {
                return true;
            }
        }
    }

    private void releaseQueueSlot() {
        int now = queuedTasks.decrementAndGet();
        if (now < 0) {
            // Best-effort: avoid underflow breaking future reservations.
            queuedTasks.set(0);
        }
    }

    private ChannelState channelState(Channel ch) {
        Attribute<ChannelState> attr = ch.attr(CHANNEL_STATE);
        ChannelState state = attr.get();
        if (state != null) {
            return state;
        }
        ChannelState created = new ChannelState();
        ChannelState existing = attr.setIfAbsent(created);
        return existing == null ? created : existing;
    }

    private void tryCompactFrameIfNeeded(ChannelHandlerContext ctx, RespCommand cmd) {
        if (frameCompactionThresholdBytes <= 0) {
            return;
        }
        if (ctx == null || cmd == null) {
            return;
        }
        RespFrame frame = cmd.frame();
        if (!(frame instanceof NettyRespFrame nettyFrame)) {
            return;
        }
        int length = nettyFrame.length();
        if (length <= 0 || length > frameCompactionMaxCopyBytes) {
            return;
        }
        int retained = nettyFrame.retainedBytes();
        if (retained <= length) {
            return;
        }
        if ((long) retained < frameCompactionThresholdBytes) {
            return;
        }
        if ((double) retained < (double) length * frameCompactionRatio) {
            return;
        }

        ByteBuf src = nettyFrame.unwrap();
        if (src == null) {
            return;
        }

        ByteBuf copied = ctx.alloc().buffer(length, length);
        boolean ok = false;
        try {
            copied.writeBytes(src, 0, length);
            RespCommandBuilder.replaceFrame(cmd, new NettyRespFrame(copied));
            ok = true;
        } finally {
            if (!ok) {
                copied.release();
            }
        }
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

        IdentityHashMap<Channel, ChannelHandlerContext> flushTargets = new IdentityHashMap<>();
        for (; ; ) {
            if (processed >= maxDrainCommands) {
                break;
            }
            if (System.nanoTime() >= deadline) {
                break;
            }

            Task task = pollTask();
            if (task == null) {
                break;
            }
            processed++;

            executeOne(task, flushTargets);
        }

        for (ChannelHandlerContext ctx : flushTargets.values()) {
            safeFlush(ctx);
        }

        if (hasPendingTasks()) {
            executor.execute(this::drainLoop);
            return;
        }

        drainScheduled.set(false);
        if (hasPendingTasks() && drainScheduled.compareAndSet(false, true)) {
            executor.execute(this::drainLoop);
        }
    }

    private boolean hasPendingTasks() {
        return taskQueue.hasPendingTasks();
    }

    private Task pollTask() {
        return taskQueue.poll();
    }

    private Task pollFairTask() {
        for (; ; ) {
            Channel ch = activeChannels.poll();
            if (ch == null) {
                return null;
            }

            ChannelState state = channelState(ch);
            Task task = state.queue.poll();
            if (task == null) {
                // The channel was scheduled but its queue is empty (may happen due to races). Unschedule it.
                state.scheduled.set(false);
                if (!state.queue.isEmpty() && state.scheduled.compareAndSet(false, true)) {
                    activeChannels.offer(ch);
                }
                continue;
            }

            if (!state.queue.isEmpty()) {
                // More work for this channel: re-queue it for round-robin fairness.
                activeChannels.offer(ch);
            } else {
                // Try to unschedule; handle the race where a new task arrives while we're draining.
                state.scheduled.set(false);
                if (!state.queue.isEmpty() && state.scheduled.compareAndSet(false, true)) {
                    activeChannels.offer(ch);
                }
            }

            return task;
        }
    }

    private void executeOne(Task task, IdentityHashMap<Channel, ChannelHandlerContext> flushTargets) {
        ChannelHandlerContext ctx = task.ctx;
        RespCommand cmd = task.cmd;
        if (ctx == null || cmd == null) {
            return;
        }
        if (isChannelClosing(ctx.channel())) {
            // QUIT 之后：只回收已入队的命令帧与预算，不再执行，避免产生副作用。
            recycleAndRelease(task);
            return;
        }

        try {
            ByteBuf out = ctx.alloc().buffer();
            boolean ok = false;
            try {
                RespWriter writer = new RespWriter(new NettyByteBufSink(out), new NettyRespSession(ctx.channel()));
                commandProcessor.execute(cmd, writer);
                if (writer.closeAfterReplyRequested()) {
                    // close-after-reply：flush 后关闭连接，并标记该 channel 后续任务需要跳过。
                    markChannelClosing(ctx.channel());
                    disableAutoRead(ctx.channel());
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
                    new RespWriter(new NettyByteBufSink(out), new NettyRespSession(ctx.channel())).error("ERR internal error");
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
            onCommandFinished(ctx.channel(), task.retainedBytes);
        }
    }

    private void onCommandFinished(Channel ch, int retainedBytes) {
        AtomicInteger pending = pendingCounter(ch);
        int now = pending.decrementAndGet();
        AtomicLong pendingBytes = pendingBytesCounter(ch);
        long bytesNow = pendingBytes.addAndGet(-retainedBytes);
        releaseQueuedBytes(retainedBytes);
        releaseQueueSlot();

        boolean pendingOk = now <= backpressureLowWatermark;
        boolean bytesOk = backpressureBytesHighWatermark <= 0 || bytesNow <= backpressureBytesLowWatermark;
        boolean globalOk = isGlobalBackpressureCleared();
        if (running && !isChannelClosing(ch) && pendingOk && bytesOk && globalOk) {
            enableAutoReadIfWeDisabled(ch);
        }
        if (running && globalOk) {
            scheduleGlobalRecovery();
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

    private static AtomicInteger pendingCounter(Channel ch) {
        Attribute<AtomicInteger> attr = ch.attr(PENDING_PER_CHANNEL);
        AtomicInteger pending = attr.get();
        if (pending != null) {
            return pending;
        }
        AtomicInteger created = new AtomicInteger();
        AtomicInteger existing = attr.setIfAbsent(created);
        return existing == null ? created : existing;
    }

    private static AtomicLong pendingBytesCounter(Channel ch) {
        Attribute<AtomicLong> attr = ch.attr(PENDING_BYTES_PER_CHANNEL);
        AtomicLong pending = attr.get();
        if (pending != null) {
            return pending;
        }
        AtomicLong created = new AtomicLong();
        AtomicLong existing = attr.setIfAbsent(created);
        return existing == null ? created : existing;
    }

    private void disableAutoRead(Channel ch) {
        if (ch == null) {
            return;
        }
        Attribute<Boolean> disabledAttr = ch.attr(AUTOREAD_DISABLED_BY_EXECUTOR);
        Boolean already = disabledAttr.get();
        if (Boolean.TRUE.equals(already)) {
            return;
        }
        disabledAttr.set(true);
        trackAutoReadDisabled(ch);
        ch.eventLoop().execute(() -> {
            try {
                ch.config().setAutoRead(false);
            } catch (Throwable ignored) {
                // ignore
            }
        });
    }

    private void enableAutoReadIfWeDisabled(Channel ch) {
        if (ch == null) {
            return;
        }
        Attribute<Boolean> disabledAttr = ch.attr(AUTOREAD_DISABLED_BY_EXECUTOR);
        if (!Boolean.TRUE.equals(disabledAttr.get())) {
            return;
        }
        disabledAttr.set(false);
        channelsWithAutoReadDisabled.remove(ch);
        ch.eventLoop().execute(() -> {
            try {
                ch.config().setAutoRead(true);
            } catch (Throwable ignored) {
                // ignore
            }
        });
    }

    private void trackAutoReadDisabled(Channel ch) {
        if (ch == null) {
            return;
        }
        if (channelsWithAutoReadDisabled.putIfAbsent(ch, Boolean.TRUE) == null) {
            ch.closeFuture().addListener(ignored -> channelsWithAutoReadDisabled.remove(ch));
        }
    }

    private boolean isGlobalBackpressureHigh() {
        boolean tasksHigh = queuedTasks.get() >= globalBackpressureHighWatermark;
        boolean bytesHigh = globalBackpressureBytesHighWatermark > 0 && queuedBytes.get() >= globalBackpressureBytesHighWatermark;
        return tasksHigh || bytesHigh;
    }

    private boolean isGlobalBackpressureCleared() {
        boolean tasksOk = queuedTasks.get() <= globalBackpressureLowWatermark;
        boolean bytesOk = globalBackpressureBytesHighWatermark <= 0 || queuedBytes.get() <= globalBackpressureBytesLowWatermark;
        return tasksOk && bytesOk;
    }

    private void scheduleGlobalRecovery() {
        if (!globalRecoveryScheduled.compareAndSet(false, true)) {
            return;
        }
        if (executor.inEventLoop()) {
            recoverGlobalAutoRead();
            return;
        }
        executor.execute(this::recoverGlobalAutoRead);
    }

    private void recoverGlobalAutoRead() {
        globalRecoveryScheduled.set(false);
        if (!running) {
            return;
        }
        if (!isGlobalBackpressureCleared()) {
            return;
        }

        for (Channel ch : channelsWithAutoReadDisabled.keySet()) {
            if (ch == null) {
                continue;
            }
            if (!ch.isActive()) {
                channelsWithAutoReadDisabled.remove(ch);
                continue;
            }
            if (isChannelClosing(ch)) {
                continue;
            }

            int pending = pendingCounter(ch).get();
            long pendingBytes = pendingBytesCounter(ch).get();
            boolean pendingOk = pending <= backpressureLowWatermark;
            boolean bytesOk = backpressureBytesHighWatermark <= 0 || pendingBytes <= backpressureBytesLowWatermark;
            if (pendingOk && bytesOk) {
                enableAutoReadIfWeDisabled(ch);
            }
        }
    }

    private static boolean isChannelClosing(Channel ch) {
        if (ch == null) {
            return false;
        }
        return Boolean.TRUE.equals(ch.attr(CHANNEL_CLOSING).get());
    }

    private static void markChannelClosing(Channel ch) {
        if (ch == null) {
            return;
        }
        ch.attr(CHANNEL_CLOSING).set(true);
    }

    private void drainLeftoverCommands() {
        taskQueue.drainLeftoverCommands();
    }

    private void recycleAndRelease(Task t) {
        if (t == null || t.cmd == null) {
            return;
        }
        try {
            t.cmd.close();
        } catch (Throwable ignored) {
            // ignore
        }
        if (t.ctx != null) {
            onCommandFinished(t.ctx.channel(), t.retainedBytes);
            return;
        }
        releaseQueuedBytes(t.retainedBytes);
        releaseQueueSlot();
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

    private boolean tryReserveQueuedBytes(int bytes) {
        if (queueMaxBytes <= 0 || bytes <= 0) {
            return true;
        }
        for (; ; ) {
            long cur = queuedBytes.get();
            long next = cur + bytes;
            if (next < 0) {
                // overflow guard: treat as OOM / reject.
                return false;
            }
            if (next > queueMaxBytes) {
                return false;
            }
            if (queuedBytes.compareAndSet(cur, next)) {
                return true;
            }
        }
    }

    private void releaseQueuedBytes(int bytes) {
        if (queueMaxBytes <= 0 || bytes <= 0) {
            return;
        }
        long now = queuedBytes.addAndGet(-bytes);
        if (now < 0) {
            // Best-effort: avoid underflow breaking future reservations.
            queuedBytes.set(0);
        }
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

    private static int defaultGlobalBackpressureHighWatermark(int queueCapacity) {
        if (queueCapacity <= 0) {
            return 1;
        }
        int high = (queueCapacity * 3 + 3) / 4; // ceil(0.75 * cap)
        if (high <= 0) {
            high = 1;
        }
        if (high > queueCapacity) {
            high = queueCapacity;
        }
        return high;
    }

    private static int defaultGlobalBackpressureLowWatermark(int globalHigh) {
        if (globalHigh <= 1) {
            return 0;
        }
        int low = globalHigh / 2;
        if (low >= globalHigh) {
            low = globalHigh - 1;
        }
        return Math.max(0, low);
    }

    private static long defaultGlobalBackpressureBytesHighWatermark(long queueMaxBytes) {
        if (queueMaxBytes <= 0) {
            return 0;
        }
        long high = (queueMaxBytes * 3) / 4;
        if (high <= 0) {
            return queueMaxBytes;
        }
        if (high > queueMaxBytes) {
            return queueMaxBytes;
        }
        return high;
    }

    private static long defaultGlobalBackpressureBytesLowWatermark(long globalBytesHigh) {
        if (globalBytesHigh <= 0) {
            return 0;
        }
        long low = globalBytesHigh / 2;
        if (low >= globalBytesHigh) {
            low = globalBytesHigh - 1;
        }
        return Math.max(0, low);
    }

    /**
     * Internal queue adapter that isolates scheduling-policy branching (GLOBAL vs FAIR).
     * <p>
     * This keeps {@link NettyCommandExecutor} focused on lifecycle/backpressure/execution, while queue mechanics
     * live in one place.
     */
    private final class TaskQueue {
        boolean offer(Channel ch, Task task) {
            if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
                return queue.offer(task);
            }
            ChannelState state = channelState(ch);
            state.queue.offer(task);
            if (state.scheduled.compareAndSet(false, true)) {
                activeChannels.offer(ch);
            }
            return true;
        }

        Task poll() {
            if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
                return queue.poll();
            }
            return pollFairTask();
        }

        boolean hasPendingTasks() {
            if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
                return queue != null && !queue.isEmpty();
            }
            return !activeChannels.isEmpty();
        }

        void drainLeftoverCommands() {
            if (schedulingPolicy == SchedulingPolicy.GLOBAL) {
                Task t;
                while ((t = queue.poll()) != null) {
                    recycleAndRelease(t);
                }
                return;
            }

            Channel ch;
            while ((ch = activeChannels.poll()) != null) {
                ChannelState state = channelState(ch);
                Task t;
                while ((t = state.queue.poll()) != null) {
                    recycleAndRelease(t);
                }
                state.scheduled.set(false);
            }
        }
    }

    private static final class ChannelState {
        private final ConcurrentLinkedQueue<Task> queue = new ConcurrentLinkedQueue<>();
        private final AtomicBoolean scheduled = new AtomicBoolean(false);
    }

    private static final class Task {
        private final ChannelHandlerContext ctx;
        private final RespCommand cmd;
        private final int retainedBytes;

        private Task(ChannelHandlerContext ctx, RespCommand cmd, int retainedBytes) {
            this.ctx = ctx;
            this.cmd = cmd;
            this.retainedBytes = retainedBytes;
        }

        static Task command(ChannelHandlerContext ctx, RespCommand cmd, int retainedBytes) {
            return new Task(ctx, cmd, retainedBytes);
        }
    }
}
