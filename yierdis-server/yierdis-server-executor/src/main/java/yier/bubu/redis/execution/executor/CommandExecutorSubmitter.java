package yier.bubu.redis.execution.executor;

import yier.bubu.redis.execution.api.ExecutionRequest;

import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;

final class CommandExecutorSubmitter<C extends ExecutionConnection> {
    private final ExecutorTaskQueue<C, CommandExecutorTask<C>> taskQueue;
    private final ExecutorBacklogBudget backlogBudget;
    private final ExecutorBackpressureController<C> backpressureController;
    private final int backpressureHighWatermark;
    private final long backpressureBytesHighWatermark;
    private final BooleanSupplier running;
    private final LongAdder submitAccepted = new LongAdder();
    private final LongAdder submitRejectedNotRunning = new LongAdder();
    private final LongAdder submitRejectedClosing = new LongAdder();
    private final LongAdder submitRejectedQueueFull = new LongAdder();
    private final LongAdder submitRejectedBytesBudget = new LongAdder();
    private final LongAdder submitRejectedRequestTooLarge = new LongAdder();
    private final LongAdder submitRejectedOfferFailed = new LongAdder();

    CommandExecutorSubmitter(
            ExecutorTaskQueue<C, CommandExecutorTask<C>> taskQueue,
            ExecutorBacklogBudget backlogBudget,
            ExecutorBackpressureController<C> backpressureController,
            int backpressureHighWatermark,
            long backpressureBytesHighWatermark,
            BooleanSupplier running
    ) {
        this.taskQueue = Objects.requireNonNull(taskQueue, "taskQueue");
        this.backlogBudget = Objects.requireNonNull(backlogBudget, "backlogBudget");
        this.backpressureController = Objects.requireNonNull(backpressureController, "backpressureController");
        this.backpressureHighWatermark = backpressureHighWatermark;
        this.backpressureBytesHighWatermark = backpressureBytesHighWatermark;
        this.running = Objects.requireNonNull(running, "running");
    }

    CommandExecutor.SubmitRejectReason trySubmit(C connection, ExecutionRequest request, Runnable scheduleDrain) {
        return trySubmit(connection, request, null, scheduleDrain);
    }

    CommandExecutor.SubmitRejectReason trySubmit(
            C connection,
            ExecutionRequest request,
            ExecutionReply reply,
            Runnable scheduleDrain
    ) {
        Objects.requireNonNull(connection, "connection");
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(scheduleDrain, "scheduleDrain");

        ExecutionConnectionContext context = connection.context();
        if (!running.getAsBoolean()) {
            context.recordCommandRejected();
            submitRejectedNotRunning.increment();
            return CommandExecutor.SubmitRejectReason.NOT_RUNNING;
        }
        if (context.isClosing()) {
            context.recordCommandRejected();
            submitRejectedClosing.increment();
            return CommandExecutor.SubmitRejectReason.CONNECTION_CLOSING;
        }

        int retainedBytes = safeRetainedBytes(request);
        if (!backlogBudget.canEverReserveQueuedBytes(retainedBytes)) {
            context.recordCommandRejected();
            submitRejectedRequestTooLarge.increment();
            return CommandExecutor.SubmitRejectReason.REQUEST_TOO_LARGE;
        }

        if (backlogBudget.isGlobalBackpressureHigh()) {
            backpressureController.disableAutoRead(connection);
        }
        if (context.pending() >= backpressureHighWatermark) {
            backpressureController.disableAutoRead(connection);
        }
        if (backpressureBytesHighWatermark > 0 && context.pendingBytes() >= backpressureBytesHighWatermark) {
            backpressureController.disableAutoRead(connection);
        }

        if (!backlogBudget.tryReserveSlot()) {
            backpressureController.disableAutoRead(connection);
            submitRejectedQueueFull.increment();
            return CommandExecutor.SubmitRejectReason.QUEUE_FULL;
        }

        boolean reservedBytes = false;
        CommandExecutorTask<C> task = null;
        boolean offered = false;
        boolean recordedEnqueue = false;
        try {
            if (!backlogBudget.tryReserveQueuedBytes(retainedBytes)) {
                backlogBudget.releaseSlot();
                backpressureController.disableAutoRead(connection);
                submitRejectedBytesBudget.increment();
                return CommandExecutor.SubmitRejectReason.BYTES_BUDGET;
            }
            reservedBytes = true;

            task = new CommandExecutorTask<>(connection, request, retainedBytes, reply);
            boolean accepted = taskQueue.offer(connection, task);
            if (!accepted) {
                backlogBudget.releaseQueuedBytes(retainedBytes);
                backlogBudget.releaseSlot();
                backpressureController.disableAutoRead(connection);
                submitRejectedOfferFailed.increment();
                return CommandExecutor.SubmitRejectReason.OFFER_FAILED;
            }
            offered = true;

            context.recordCommandEnqueued(retainedBytes);
            recordedEnqueue = true;
            if (context.pending() >= backpressureHighWatermark) {
                backpressureController.disableAutoRead(connection);
            }
            if (backpressureBytesHighWatermark > 0 && context.pendingBytes() >= backpressureBytesHighWatermark) {
                backpressureController.disableAutoRead(connection);
            }
            if (backlogBudget.isGlobalBackpressureHigh()) {
                backpressureController.disableAutoRead(connection);
            }
            scheduleDrain.run();
            submitAccepted.increment();
            return null;
        } catch (Throwable ignored) {
            if (offered) {
                boolean removed;
                try {
                    removed = taskQueue.remove(connection, task);
                } catch (Throwable removalFailure) {
                    // 无法证明任务仍在队列时，所有权必须保守地留给 executor。
                    submitAccepted.increment();
                    return null;
                }
                if (!removed) {
                    // drain 可能已经取得任务；此时调用方不能再关闭 request/reply。
                    submitAccepted.increment();
                    return null;
                }
                if (recordedEnqueue) {
                    context.rollbackCommandEnqueued(retainedBytes);
                }
            }
            if (reservedBytes) {
                backlogBudget.releaseQueuedBytes(retainedBytes);
            }
            backlogBudget.releaseSlot();
            submitRejectedOfferFailed.increment();
            return CommandExecutor.SubmitRejectReason.OFFER_FAILED;
        }
    }

    long submitAccepted() {
        return submitAccepted.sum();
    }

    long submitRejectedNotRunning() {
        return submitRejectedNotRunning.sum();
    }

    long submitRejectedClosing() {
        return submitRejectedClosing.sum();
    }

    long submitRejectedQueueFull() {
        return submitRejectedQueueFull.sum();
    }

    long submitRejectedBytesBudget() {
        return submitRejectedBytesBudget.sum();
    }

    long submitRejectedRequestTooLarge() {
        return submitRejectedRequestTooLarge.sum();
    }

    long submitRejectedOfferFailed() {
        return submitRejectedOfferFailed.sum();
    }

    private static int safeRetainedBytes(ExecutionRequest request) {
        if (request == null) {
            return 0;
        }
        try {
            return Math.max(0, request.retainedBytes());
        } catch (Throwable ignored) {
            return 0;
        }
    }
}
