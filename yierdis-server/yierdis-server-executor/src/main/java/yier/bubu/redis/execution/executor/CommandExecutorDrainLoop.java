package yier.bubu.redis.execution.executor;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

final class CommandExecutorDrainLoop<C extends ExecutionConnection> {
    private final Executor ownerExecutor;
    private final ExecutorTaskQueue<C, CommandExecutorTask<C>> taskQueue;
    private final CommandExecutorExecutionSupport<C> executionSupport;
    private final int maxDrainCommands;
    private final long drainTimeLimitNanos;
    private final BooleanSupplier running;
    private final LongAdder drainLimitedByMaxCommands = new LongAdder();
    private final LongAdder drainLimitedByTimeBudget = new LongAdder();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean drainScheduled = new AtomicBoolean(false);

    CommandExecutorDrainLoop(
            Executor ownerExecutor,
            ExecutorTaskQueue<C, CommandExecutorTask<C>> taskQueue,
            CommandExecutorExecutionSupport<C> executionSupport,
            int maxDrainCommands,
            long drainTimeLimitNanos,
            BooleanSupplier running
    ) {
        this.ownerExecutor = Objects.requireNonNull(ownerExecutor, "ownerExecutor");
        this.taskQueue = Objects.requireNonNull(taskQueue, "taskQueue");
        this.executionSupport = Objects.requireNonNull(executionSupport, "executionSupport");
        this.maxDrainCommands = maxDrainCommands;
        this.drainTimeLimitNanos = drainTimeLimitNanos;
        this.running = Objects.requireNonNull(running, "running");
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
        submitDrainTask();
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
        LinkedHashSet<C> touchedConnections = new LinkedHashSet<>();

        for (; ; ) {
            if (processed >= maxDrainCommands) {
                hitMaxDrainCommands = true;
                break;
            }
            if (System.nanoTime() >= deadline) {
                hitDrainTimeBudget = true;
                break;
            }

            CommandExecutorTask<C> task = taskQueue.poll();
            if (task == null) {
                break;
            }
            processed++;
            ExecutionAttempt attempt = executionSupport.execute(task, touchedConnections);
            if (attempt == ExecutionAttempt.REPLY_CAPACITY_BLOCKED) {
                registerBlockedReplyTask(task);
            }
        }

        executionSupport.flushPending(touchedConnections);

        boolean runnableAfterDrain = taskQueue.hasRunnableTasks();
        if (runnableAfterDrain) {
            if (hitMaxDrainCommands) {
                drainLimitedByMaxCommands.increment();
            } else if (hitDrainTimeBudget) {
                drainLimitedByTimeBudget.increment();
            }
        }
        if (runnableAfterDrain) {
            submitDrainTask();
            return;
        }

        drainScheduled.set(false);
        if (taskQueue.hasRunnableTasks() && drainScheduled.compareAndSet(false, true)) {
            submitDrainTask();
        }
    }

    private void submitDrainTask() {
        try {
            ownerExecutor.execute(this::drainLoop);
        } catch (RuntimeException | Error failure) {
            // execute 拒绝时没有新的 drain 接管队列；归还标记后，调用方或后续唤醒才能重新调度已接受任务。
            drainScheduled.set(false);
            throw failure;
        }
    }

    private void registerBlockedReplyTask(CommandExecutorTask<C> task) {
        if (task == null || task.connection == null || task.reply == null || !taskQueue.block(task.connection, task)) {
            executionSupport.recycleAndRelease(task);
            return;
        }

        boolean waiting;
        try {
            waiting = task.reply.awaitCapacity(() -> {
                if (taskQueue.resumeBlocked(task.connection, task)) {
                    scheduleDrain();
                }
            });
        } catch (Throwable ignored) {
            waiting = false;
        }
        if (!waiting) {
            if (taskQueue.cancelBlocked(task.connection, task)) {
                executionSupport.recycleAndRelease(task);
            }
            return;
        }

        executionSupport.onConnectionClosed(task.connection, () -> ownerExecutor.execute(() -> {
            if (taskQueue.cancelBlocked(task.connection, task)) {
                executionSupport.recycleAndRelease(task);
            }
        }));
    }

    long drainLimitedByMaxCommands() {
        return drainLimitedByMaxCommands.sum();
    }

    long drainLimitedByTimeBudget() {
        return drainLimitedByTimeBudget.sum();
    }
}
