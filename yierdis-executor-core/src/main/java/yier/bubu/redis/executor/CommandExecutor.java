package yier.bubu.redis.executor;

import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriterFactory;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public final class CommandExecutor<C extends ExecutionConnection> implements AutoCloseable {
    public enum SubmitRejectReason {
        NOT_RUNNING("not_running"),
        QUEUE_FULL("queue_full"),
        BYTES_BUDGET("bytes_budget"),
        OFFER_FAILED("offer_failed");

        private final String code;

        SubmitRejectReason(String code) {
            this.code = code == null ? "unknown" : code;
        }

        public String code() {
            return code;
        }
    }

    private final Executor ownerExecutor;
    private final ExecutorBacklogBudget backlogBudget;
    private final ExecutorTaskQueue<C, CommandExecutorTask<C>> taskQueue;
    private final CommandExecutorSubmitter<C> submitter;
    private final CommandExecutorDrainLoop<C> drainLoop;
    private final CommandExecutorExecutionSupport<C> executionSupport;
    private final SchedulingPolicy schedulingPolicy;
    private volatile boolean running = true;

    public CommandExecutor(
            Runnable bindToCurrentThread,
            YierdisFastCommandProcessor commandProcessor,
            Executor ownerExecutor,
            ReplyWriterFactory replyWriterFactory,
            ExecutionIoAdapter<C> ioAdapter,
            CommandExecutorConfig config
    ) {
        Objects.requireNonNull(bindToCurrentThread, "bindToCurrentThread");
        Objects.requireNonNull(config, "config");
        this.ownerExecutor = Objects.requireNonNull(ownerExecutor, "ownerExecutor");
        this.schedulingPolicy = config.schedulingPolicy();
        this.backlogBudget = new ExecutorBacklogBudget(config.queueCapacity(), config.queueMaxBytes());

        ArrayBlockingQueue<CommandExecutorTask<C>> globalQueue = this.schedulingPolicy == SchedulingPolicy.GLOBAL
                ? new ArrayBlockingQueue<>(config.queueCapacity())
                : null;
        this.taskQueue = new ExecutorTaskQueue<>(this.schedulingPolicy, globalQueue, CommandExecutor::queueStateFor);
        this.executionSupport = new CommandExecutorExecutionSupport<>(commandProcessor, replyWriterFactory, ioAdapter, backlogBudget);
        this.submitter = new CommandExecutorSubmitter<>(taskQueue, backlogBudget, () -> running);
        this.drainLoop = new CommandExecutorDrainLoop<>(
                this.ownerExecutor,
                taskQueue,
                executionSupport,
                config.maxDrainCommands(),
                TimeUnit.MILLISECONDS.toNanos(config.drainTimeLimitMillis()),
                () -> running
        );
        this.ownerExecutor.execute(bindToCurrentThread);
    }

    public void start() {
        drainLoop.markStarted();
    }

    public SubmitRejectReason trySubmit(C connection, ExecutionRequest request) {
        return submitter.trySubmit(connection, request, drainLoop::scheduleDrain);
    }

    public StatsSnapshot statsSnapshot() {
        return executionSupport.statsSnapshot(backlogBudget.queuedTasks(), backlogBudget.queuedBytes(), schedulingPolicy);
    }

    public void executeMaintenance(Runnable task) {
        Objects.requireNonNull(task, "task");
        ownerExecutor.execute(() -> {
            if (running) {
                task.run();
            }
        });
    }

    public CompletableFuture<Void> shutdownGracefully() {
        running = false;
        CompletableFuture<Void> future = new CompletableFuture<>();
        ownerExecutor.execute(() -> {
            drainLoop.drainLeftoverCommands();
            future.complete(null);
        });
        return future;
    }

    @Override
    public void close() {
        running = false;
        drainLoop.drainLeftoverCommands();
    }

    public record StatsSnapshot(
            long commandsExecuted,
            long commandsSkippedClosing,
            int queuedTasks,
            long queuedBytes,
            SchedulingPolicy schedulingPolicy
    ) {
    }

    @SuppressWarnings("unchecked")
    private static <C extends ExecutionConnection> ExecutorKeyState<CommandExecutorTask<C>> queueStateFor(C connection) {
        return (ExecutorKeyState<CommandExecutorTask<C>>) (ExecutorKeyState<?>) connection.context().queueState();
    }
}
