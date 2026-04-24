package yier.bubu.redis.executor;

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
        ownerExecutor.execute(this::drainLoop);
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
            executionSupport.execute(task, touchedConnections);
        }

        executionSupport.flushPending(touchedConnections);

        boolean pendingAfterDrain = taskQueue.hasPendingTasks();
        if (pendingAfterDrain) {
            if (hitMaxDrainCommands) {
                drainLimitedByMaxCommands.increment();
            } else if (hitDrainTimeBudget) {
                drainLimitedByTimeBudget.increment();
            }
        }
        if (pendingAfterDrain) {
            ownerExecutor.execute(this::drainLoop);
            return;
        }

        drainScheduled.set(false);
        if (taskQueue.hasPendingTasks() && drainScheduled.compareAndSet(false, true)) {
            ownerExecutor.execute(this::drainLoop);
        }
    }

    long drainLimitedByMaxCommands() {
        return drainLimitedByMaxCommands.sum();
    }

    long drainLimitedByTimeBudget() {
        return drainLimitedByTimeBudget.sum();
    }
}
