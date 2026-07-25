package yier.bubu.redis.execution.executor;

import yier.bubu.redis.execution.api.CapacityRegistration;
import yier.bubu.redis.execution.api.RedisReplyWriterFactory;
import yier.bubu.redis.execution.api.ReplySizer;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

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
    private final LongAdder backpressureEnter = new LongAdder();
    private final LongAdder backpressureExit = new LongAdder();
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
                backpressureIo(ioAdapter),
                backpressureRuntime(),
                backpressureObserver(),
                () -> running
        );

        ArrayBlockingQueue<CommandExecutorTask<C>> globalQueue = this.schedulingPolicy == SchedulingPolicy.GLOBAL
                ? new ArrayBlockingQueue<>(config.queueCapacity())
                : null;
        this.taskQueue = new ExecutorTaskQueue<>(this.schedulingPolicy, globalQueue, CommandExecutor::queueStateFor);
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
                config.backpressureHighWatermark(),
                config.backpressureBytesHighWatermark(),
                () -> running,
                drainLoop::scheduleDrain
        );
    }

    private ExecutorBackpressureIo<C> backpressureIo(ExecutionIoAdapter<C> ioAdapter) {
        return new ExecutorBackpressureIo<>() {
            @Override public boolean isActive(C key) { return ioAdapter.isActive(key); }
            @Override public boolean isWritable(C key) { return ioAdapter.isWritable(key); }
            @Override public void disableAutoRead(C key) { ioAdapter.disableInput(key); }
            @Override public void enableAutoRead(C key) { ioAdapter.enableInput(key); }
            @Override public void onClose(C key, Runnable callback) { ioAdapter.onClose(key, callback); }
        };
    }

    private ExecutorBackpressureRuntime<C> backpressureRuntime() {
        return new ExecutorBackpressureRuntime<>() {
            @Override public int pending(C key) { return key.context().pending(); }
            @Override public long pendingBytes(C key) { return key.context().pendingBytes(); }
            @Override public boolean isClosing(C key) { return key.context().isClosing(); }
            @Override public boolean markAutoReadDisabledByExecutor(C key) {
                return key.context().markInputDisabledByExecutor();
            }
            @Override public boolean autoReadDisabledByExecutor(C key) {
                return key.context().autoReadDisabledByExecutor();
            }
            @Override public boolean clearAutoReadDisabledByExecutor(C key) {
                return key.context().clearAutoReadDisabledByExecutor();
            }
            @Override public boolean inputPausedByReply(C key) { return key.context().inputPausedByReply(); }
        };
    }

    private ExecutorBackpressureObserver<C> backpressureObserver() {
        return new ExecutorBackpressureObserver<>() {
            @Override
            public void onEnter(C key) {
                key.context().recordBackpressureEnter();
                backpressureEnter.increment();
            }

            @Override
            public void onExit(C key) {
                key.context().recordBackpressureExit();
                backpressureExit.increment();
            }
        };
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
                backpressureController.keysAutoReadDisabledCount(),
                submitter.submitAccepted(),
                submitter.submitRejectedNotRunning(),
                submitter.submitRejectedClosing(),
                submitter.submitRejectedQueueFull(),
                submitter.submitRejectedBytesBudget(),
                submitter.submitRejectedRequestTooLarge(),
                submitter.submitRejectedOfferFailed(),
                executionSupport.closeAfterReply(),
                backpressureEnter.sum(),
                backpressureExit.sum(),
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

    @SuppressWarnings("unchecked")
    private static <C extends ExecutionConnection> ExecutorKeyState<CommandExecutorTask<C>> queueStateFor(C connection) {
        return (ExecutorKeyState<CommandExecutorTask<C>>) (ExecutorKeyState<?>) connection.context().queueState();
    }
}
