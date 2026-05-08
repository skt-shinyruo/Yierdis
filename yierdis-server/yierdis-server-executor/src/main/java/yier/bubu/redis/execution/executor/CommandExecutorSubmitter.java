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
    private final LongAdder submitRejectedQueueFull = new LongAdder();
    private final LongAdder submitRejectedBytesBudget = new LongAdder();
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
        Objects.requireNonNull(connection, "connection");
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(scheduleDrain, "scheduleDrain");

        ExecutionConnectionContext context = connection.context();
        if (!running.getAsBoolean()) {
            context.recordCommandRejected();
            submitRejectedNotRunning.increment();
            return CommandExecutor.SubmitRejectReason.NOT_RUNNING;
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
            context.recordCommandRejected();
            submitRejectedQueueFull.increment();
            return CommandExecutor.SubmitRejectReason.QUEUE_FULL;
        }

        int retainedBytes = safeRetainedBytes(request);
        boolean reservedBytes = false;
        try {
            if (!backlogBudget.tryReserveQueuedBytes(retainedBytes)) {
                backlogBudget.releaseSlot();
                backpressureController.disableAutoRead(connection);
                context.recordCommandRejected();
                submitRejectedBytesBudget.increment();
                return CommandExecutor.SubmitRejectReason.BYTES_BUDGET;
            }
            reservedBytes = true;

            boolean accepted = taskQueue.offer(connection, new CommandExecutorTask<>(connection, request, retainedBytes));
            if (!accepted) {
                backlogBudget.releaseQueuedBytes(retainedBytes);
                backlogBudget.releaseSlot();
                backpressureController.disableAutoRead(connection);
                context.recordCommandRejected();
                submitRejectedOfferFailed.increment();
                return CommandExecutor.SubmitRejectReason.OFFER_FAILED;
            }

            context.recordCommandEnqueued(retainedBytes);
            submitAccepted.increment();
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
            return null;
        } catch (Throwable ignored) {
            if (reservedBytes) {
                backlogBudget.releaseQueuedBytes(retainedBytes);
            }
            backlogBudget.releaseSlot();
            context.recordCommandRejected();
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

    long submitRejectedQueueFull() {
        return submitRejectedQueueFull.sum();
    }

    long submitRejectedBytesBudget() {
        return submitRejectedBytesBudget.sum();
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
