package yier.bubu.redis.execution.executor;

import yier.bubu.redis.execution.api.CapacityRegistration;
import yier.bubu.redis.execution.api.RedisReplyWriterFactory;
import yier.bubu.redis.execution.api.ReplySizer;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public final class CommandExecutor<C extends ExecutionConnection> implements AutoCloseable {
    private final Runnable bindToCurrentThread;
    public enum SubmitRejectReason {
        NOT_RUNNING("not_running"),
        CONNECTION_CLOSING("connection_closing"),
        QUEUE_FULL("queue_full"),
        BYTES_BUDGET("bytes_budget"),
        REQUEST_TOO_LARGE("request_too_large"),
        OFFER_FAILED("offer_failed");

        private final String code;

        SubmitRejectReason(String code) {
            this.code = code == null ? "unknown" : code;
        }

        public String code() {
            return code;
        }
    }

    private final SerialOwnerExecutor ownerExecutor;
    private final ExecutorBacklogBudget backlogBudget;
    private final ExecutorBackpressureController<C> backpressureController;
    private final ExecutorTaskQueue<C, CommandExecutorTask<C>> taskQueue;
    private final CommandExecutorSubmitter<C> submitter;
    private final CommandExecutorDrainLoop<C> drainLoop;
    private final CommandExecutorExecutionSupport<C> executionSupport;
    private final SchedulingPolicy schedulingPolicy;
    private volatile boolean running = true;

    public CommandExecutor(
            Runnable bindToCurrentThread,
            CommandExecutionEngine commandProcessor,
            SerialOwnerExecutor ownerExecutor,
            ReplySizer replySizer,
            RedisReplyWriterFactory replyWriterFactory,
            ExecutionIoAdapter<C> ioAdapter,
            CommandExecutorConfig config
    ) {
        this.bindToCurrentThread = Objects.requireNonNull(bindToCurrentThread, "bindToCurrentThread");
        Objects.requireNonNull(config, "config");
        this.ownerExecutor = Objects.requireNonNull(ownerExecutor, "ownerExecutor");
        this.schedulingPolicy = config.schedulingPolicy();
        this.backlogBudget = new ExecutorBacklogBudget(config.queueCapacity(), config.queueMaxBytes());
        this.backpressureController = new ExecutorBackpressureController<>(
                this.ownerExecutor,
                backlogBudget,
                config.backpressureLowWatermark(),
                config.backpressureBytesHighWatermark(),
                config.backpressureBytesLowWatermark(),
                ioAdapter,
                () -> running
        );

        this.taskQueue = new ExecutorTaskQueue<>(this.schedulingPolicy);
        this.executionSupport = new CommandExecutorExecutionSupport<>(
                commandProcessor,
                replySizer,
                replyWriterFactory,
                ioAdapter,
                backlogBudget,
                backpressureController,
                config.backpressureLowWatermark(),
                config.backpressureBytesHighWatermark(),
                config.backpressureBytesLowWatermark(),
                () -> running
        );
        this.drainLoop = new CommandExecutorDrainLoop<>(
                this.ownerExecutor,
                taskQueue,
                executionSupport,
                config.maxDrainCommands(),
                TimeUnit.MILLISECONDS.toNanos(config.drainTimeLimitMillis()),
                () -> running
        );
        this.submitter = new CommandExecutorSubmitter<>(
                taskQueue,
                backlogBudget,
                backpressureController,
                ioAdapter,
                config.backpressureHighWatermark(),
                config.backpressureBytesHighWatermark(),
                () -> running,
                drainLoop::scheduleDrain
        );
    }

    public void start() {
        executeOwnerTask(bindToCurrentThread).join();
        drainLoop.markStarted();
    }

    public ExecutorAdmissionAttempt<C> tryAcquire(C connection, int retainedBytes) {
        return submitter.tryAcquire(connection, retainedBytes);
    }

    public CapacityRegistration onAdmissionAvailable(int retainedBytes, Runnable callback) {
        Objects.requireNonNull(callback, "callback");
        if (!running) {
            callback.run();
            return CapacityRegistration.NONE;
        }
        return backlogBudget.onCapacityAvailable(retainedBytes, callback);
    }

    public StatsSnapshot statsSnapshot() {
        return new StatsSnapshot(
                executionSupport.commandsExecuted(),
                executionSupport.commandsSkippedClosing(),
                backlogBudget.queuedTasks(),
                backlogBudget.queuedBytes(),
                schedulingPolicy,
                backpressureController.connectionsAutoReadDisabledCount(),
                submitter.submitAccepted(),
                submitter.submitRejectedNotRunning(),
                submitter.submitRejectedClosing(),
                submitter.submitRejectedQueueFull(),
                submitter.submitRejectedBytesBudget(),
                submitter.submitRejectedRequestTooLarge(),
                submitter.submitRejectedOfferFailed(),
                executionSupport.closeAfterReply(),
                backpressureController.backpressureEnter(),
                backpressureController.backpressureExit(),
                drainLoop.drainLimitedByMaxCommands(),
                drainLoop.drainLimitedByTimeBudget(),
                taskQueue.deferredFairHeads(),
                taskQueue.deferredGlobalHeads()
        );
    }

    public void executeMaintenance(Runnable task) {
        Objects.requireNonNull(task, "task");
        ownerExecutor.execute(() -> {
            ownerExecutor.requireOwnerThread();
            if (running) {
                task.run();
            }
        });
    }

    public CompletableFuture<Void> executeOwnerTask(Runnable task) {
        Objects.requireNonNull(task, "task");
        CompletableFuture<Void> future = new CompletableFuture<>();
        ownerExecutor.execute(() -> {
            ownerExecutor.requireOwnerThread();
            try {
                task.run();
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future;
    }

    public void onTransportUnwritable(C connection) {
        if (connection == null || !running) {
            return;
        }
        backpressureController.disableAutoRead(connection);
    }

    public void onTransportWritable(C connection) {
        if (connection == null || !running) {
            return;
        }
        ownerExecutor.execute(() -> {
            ownerExecutor.requireOwnerThread();
            executionSupport.recoverInputIfPossible(connection);
        });
    }

    public CompletableFuture<Void> shutdownGracefully() {
        running = false;
        backlogBudget.wakeAllCapacityWaiters();
        CompletableFuture<Void> future = new CompletableFuture<>();
        ownerExecutor.execute(() -> {
            ownerExecutor.requireOwnerThread();
            drainLoop.drainLeftoverCommands();
            future.complete(null);
        });
        return future;
    }

    @Override
    public void close() {
        running = false;
        backlogBudget.wakeAllCapacityWaiters();
        if (ownerExecutor.inOwnerThread()) {
            drainLoop.drainLeftoverCommands();
        } else {
            ownerExecutor.execute(drainLoop::drainLeftoverCommands);
        }
    }

    public record StatsSnapshot(
            long commandsExecuted,
            long commandsSkippedClosing,
            int queuedTasks,
            long queuedBytes,
            SchedulingPolicy schedulingPolicy,
            int channelsAutoReadDisabled,
            long submitAccepted,
            long submitRejectedNotRunning,
            long submitRejectedClosing,
            long submitRejectedQueueFull,
            long submitRejectedBytesBudget,
            long submitRejectedRequestTooLarge,
            long submitRejectedOfferFailed,
            long closeAfterReply,
            long backpressureEnter,
            long backpressureExit,
            long drainLimitedByMaxCommands,
            long drainLimitedByTimeBudget,
            long deferredFairReplyHeads,
            long deferredGlobalReplyHeads
    ) {
    }
}
