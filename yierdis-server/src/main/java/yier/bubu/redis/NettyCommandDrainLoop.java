package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.EventExecutor;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.executor.ExecutorTaskQueue;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;

final class NettyCommandDrainLoop {
    private final EventExecutor executor;
    private final ExecutorTaskQueue<Channel, NettyExecutorTask> taskQueue;
    private final NettyCommandExecutionSupport executionSupport;
    private final int maxDrainCommands;
    private final long drainTimeLimitNanos;
    private final BooleanSupplier running;
    private final LongAdder drainLimitedByMaxCommands;
    private final LongAdder drainLimitedByTimeBudget;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean drainScheduled = new AtomicBoolean(false);

    NettyCommandDrainLoop(
            EventExecutor executor,
            ExecutorTaskQueue<Channel, NettyExecutorTask> taskQueue,
            NettyCommandExecutionSupport executionSupport,
            int maxDrainCommands,
            long drainTimeLimitNanos,
            BooleanSupplier running,
            LongAdder drainLimitedByMaxCommands,
            LongAdder drainLimitedByTimeBudget
    ) {
        this.executor = Objects.requireNonNull(executor, "executor");
        this.taskQueue = Objects.requireNonNull(taskQueue, "taskQueue");
        this.executionSupport = Objects.requireNonNull(executionSupport, "executionSupport");
        this.maxDrainCommands = maxDrainCommands;
        this.drainTimeLimitNanos = drainTimeLimitNanos;
        this.running = Objects.requireNonNull(running, "running");
        this.drainLimitedByMaxCommands = Objects.requireNonNull(drainLimitedByMaxCommands, "drainLimitedByMaxCommands");
        this.drainLimitedByTimeBudget = Objects.requireNonNull(drainLimitedByTimeBudget, "drainLimitedByTimeBudget");
    }

    void markStarted() {
        started.set(true);
    }

    void scheduleDrain() {
        if (running.getAsBoolean() && !started.get()) {
            return;
        }
        if (!drainScheduled.compareAndSet(false, true)) {
            return;
        }
        executor.execute(this::drainLoop);
    }

    void drainLeftoverCommands() {
        taskQueue.drainLeftoverTasks(executionSupport::recycleAndRelease);
    }

    private void drainLoop() {
        if (!running.getAsBoolean()) {
            drainScheduled.set(false);
            drainLeftoverCommands();
            return;
        }

        long deadline = System.nanoTime() + drainTimeLimitNanos;
        int processed = 0;
        boolean hitMaxDrainCommands = false;
        boolean hitDrainTimeBudget = false;

        NettyReplyFlushBatch flushBatch = new NettyReplyFlushBatch();
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

            executeOne(task, flushBatch);
        }

        flushBatch.flushAll();

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

    private void executeOne(NettyExecutorTask task, NettyReplyFlushBatch flushBatch) {
        ChannelHandlerContext ctx = task.ctx;
        ExecutionRequest request = task.request;
        if (ctx == null || request == null) {
            return;
        }
        Channel ch = ctx.channel();
        if (ch == null || !ch.isActive() || NettyCommandExecutionSupport.isChannelClosing(ch)) {
            // 连接已关闭或标记为 closing：只回收已入队的命令帧与预算，不再执行，避免产生副作用。
            executionSupport.recordSkippedClosing(ch);
            executionSupport.recycleAndRelease(task);
            return;
        }

        try {
            ByteBuf out = ctx.alloc().buffer();
            boolean ok = false;
            try {
                ReplyWriter writer = executionSupport.newReplyWriter(out, ch);
                executionSupport.executeCommand(request, ch, writer);
                if (writer.closeAfterReplyRequested()) {
                    // close-after-reply：flush 后关闭连接，并标记该 channel 后续任务需要跳过。
                    executionSupport.markCloseAfterReply(ch);
                    ctx.writeAndFlush(out).addListener(ChannelFutureListener.CLOSE);
                } else {
                    ctx.write(out, ctx.voidPromise());
                    flushBatch.record(ctx);
                }
                ok = true;
            } finally {
                if (!ok) {
                    out.release();
                }
            }
        } catch (Throwable t) {
            // Internal error on the executor thread: mark closing and close the connection to avoid side effects
            // from already-queued commands (align with handler.exceptionCaught behavior).
            executionSupport.handleExecutionFailure(ctx, ch, t);
        } finally {
            try {
                request.close();
            } catch (Throwable ignored) {
                // ignore
            }
            executionSupport.onCommandFinished(ctx.channel(), task.retainedBytes, true);
        }
    }
}
