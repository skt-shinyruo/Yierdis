package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.offheap.netty.YierdisNettyByteBufSink;
import yier.bubu.redis.protocol.NettyRespSession;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespWriter;

import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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

    private static final AttributeKey<AtomicInteger> PENDING_PER_CHANNEL =
            AttributeKey.valueOf("yierdis.exec.pending");
    private static final AttributeKey<Boolean> AUTOREAD_DISABLED_BY_EXECUTOR =
            AttributeKey.valueOf("yierdis.exec.autoreadDisabled");

    private final YierdisDb db;
    private final YierdisFastCommandProcessor commandProcessor;
    private final EventExecutor executor;

    private final ArrayBlockingQueue<Task> queue;

    private final int backpressureHighWatermark;
    private final int backpressureLowWatermark;
    private final int maxDrainCommands;
    private final long drainTimeLimitNanos;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean drainScheduled = new AtomicBoolean(false);
    private volatile boolean running = true;

    public NettyCommandExecutor(
            YierdisDb db,
            YierdisFastCommandProcessor commandProcessor,
            EventExecutor executor,
            int queueCapacity,
            int backpressureHighWatermark,
            int backpressureLowWatermark,
            int maxDrainCommands,
            long drainTimeLimitMillis
    ) {
        this.db = Objects.requireNonNull(db, "db");
        this.commandProcessor = Objects.requireNonNull(commandProcessor, "commandProcessor");
        this.executor = Objects.requireNonNull(executor, "executor");
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be > 0");
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
        if (maxDrainCommands <= 0) {
            throw new IllegalArgumentException("maxDrainCommands must be > 0");
        }
        if (drainTimeLimitMillis <= 0) {
            throw new IllegalArgumentException("drainTimeLimitMillis must be > 0");
        }
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.backpressureHighWatermark = backpressureHighWatermark;
        this.backpressureLowWatermark = backpressureLowWatermark;
        this.maxDrainCommands = maxDrainCommands;
        this.drainTimeLimitNanos = TimeUnit.MILLISECONDS.toNanos(drainTimeLimitMillis);
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

        AtomicInteger pending = pendingCounter(ctx.channel());
        if (pending.get() >= backpressureHighWatermark) {
            disableAutoRead(ctx.channel());
            return false;
        }

        boolean accepted = queue.offer(Task.command(ctx, cmd));
        if (!accepted) {
            return false;
        }

        int now = pending.incrementAndGet();
        if (now >= backpressureHighWatermark) {
            disableAutoRead(ctx.channel());
        }

        scheduleDrain();
        return true;
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

        IdentityHashMap<Channel, ChannelHandlerContext> flushTargets = new IdentityHashMap<>();
        for (; ; ) {
            if (processed >= maxDrainCommands) {
                break;
            }
            if (System.nanoTime() >= deadline) {
                break;
            }

            Task task = queue.poll();
            if (task == null) {
                break;
            }
            processed++;

            executeOne(task, flushTargets);
        }

        for (ChannelHandlerContext ctx : flushTargets.values()) {
            safeFlush(ctx);
        }

        if (!queue.isEmpty()) {
            executor.execute(this::drainLoop);
            return;
        }

        drainScheduled.set(false);
        if (!queue.isEmpty() && drainScheduled.compareAndSet(false, true)) {
            executor.execute(this::drainLoop);
        }
    }

    private void executeOne(Task task, IdentityHashMap<Channel, ChannelHandlerContext> flushTargets) {
        if (task.maintenance != null) {
            try {
                task.maintenance.run();
            } catch (Throwable t) {
                log.debug("Maintenance task failed", t);
            }
            return;
        }

        ChannelHandlerContext ctx = task.ctx;
        RespCommand cmd = task.cmd;
        if (ctx == null || cmd == null) {
            return;
        }

        try {
            ByteBuf out = ctx.alloc().buffer();
            boolean ok = false;
            try {
                RespWriter writer = new RespWriter(new YierdisNettyByteBufSink(out), new NettyRespSession(ctx.channel()));
                commandProcessor.execute(cmd, writer);
                ctx.write(out, ctx.voidPromise());
                flushTargets.put(ctx.channel(), ctx);
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
                    new RespWriter(new YierdisNettyByteBufSink(out), new NettyRespSession(ctx.channel())).error("ERR internal error");
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
                cmd.recycle();
            } catch (Throwable ignored) {
                // ignore
            }
            onCommandFinished(ctx.channel());
        }
    }

    private void onCommandFinished(Channel ch) {
        AtomicInteger pending = pendingCounter(ch);
        int now = pending.decrementAndGet();
        if (now <= backpressureLowWatermark) {
            enableAutoReadIfWeDisabled(ch);
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

    private static void disableAutoRead(Channel ch) {
        if (ch == null) {
            return;
        }
        Attribute<Boolean> disabledAttr = ch.attr(AUTOREAD_DISABLED_BY_EXECUTOR);
        Boolean already = disabledAttr.get();
        if (Boolean.TRUE.equals(already)) {
            return;
        }
        disabledAttr.set(true);
        ch.eventLoop().execute(() -> {
            try {
                ch.config().setAutoRead(false);
            } catch (Throwable ignored) {
                // ignore
            }
        });
    }

    private static void enableAutoReadIfWeDisabled(Channel ch) {
        if (ch == null) {
            return;
        }
        Attribute<Boolean> disabledAttr = ch.attr(AUTOREAD_DISABLED_BY_EXECUTOR);
        if (!Boolean.TRUE.equals(disabledAttr.get())) {
            return;
        }
        disabledAttr.set(false);
        ch.eventLoop().execute(() -> {
            try {
                ch.config().setAutoRead(true);
            } catch (Throwable ignored) {
                // ignore
            }
        });
    }

    private void drainLeftoverCommands() {
        Task t;
        while ((t = queue.poll()) != null) {
            if (t.cmd != null) {
                try {
                    t.cmd.recycle();
                } catch (Throwable ignored) {
                    // ignore
                }
            }
        }
    }

    @Override
    public void close() {
        running = false;
        scheduleDrain();
    }

    private static final class Task {
        private final ChannelHandlerContext ctx;
        private final RespCommand cmd;
        private final Runnable maintenance;

        private Task(ChannelHandlerContext ctx, RespCommand cmd, Runnable maintenance) {
            this.ctx = ctx;
            this.cmd = cmd;
            this.maintenance = maintenance;
        }

        static Task command(ChannelHandlerContext ctx, RespCommand cmd) {
            return new Task(ctx, cmd, null);
        }

        @SuppressWarnings("unused")
        static Task maintenance(Runnable task) {
            return new Task(null, null, task);
        }
    }
}
