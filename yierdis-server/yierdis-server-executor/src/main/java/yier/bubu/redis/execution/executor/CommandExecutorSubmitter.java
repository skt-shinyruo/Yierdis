package yier.bubu.redis.execution.executor;

import yier.bubu.redis.execution.api.ExecutionReply;
import yier.bubu.redis.execution.api.ExecutionRequest;

import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;

final class CommandExecutorSubmitter<C extends ExecutionConnection> {
    private final ExecutorTaskQueue<C, CommandExecutorTask<C>> taskQueue;
    private final ExecutorBacklogBudget backlogBudget;
    private final ExecutorBackpressureController<C> backpressureController;
    private final ExecutionIoAdapter<C> ioAdapter;
    private final int backpressureHighWatermark;
    private final long backpressureBytesHighWatermark;
    private final BooleanSupplier running;
    private final Runnable scheduleDrain;
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
            ExecutionIoAdapter<C> ioAdapter,
            int backpressureHighWatermark,
            long backpressureBytesHighWatermark,
            BooleanSupplier running,
            Runnable scheduleDrain
    ) {
        this.taskQueue = Objects.requireNonNull(taskQueue, "taskQueue");
        this.backlogBudget = Objects.requireNonNull(backlogBudget, "backlogBudget");
        this.backpressureController = Objects.requireNonNull(backpressureController, "backpressureController");
        this.ioAdapter = Objects.requireNonNull(ioAdapter, "ioAdapter");
        this.backpressureHighWatermark = backpressureHighWatermark;
        this.backpressureBytesHighWatermark = backpressureBytesHighWatermark;
        this.running = Objects.requireNonNull(running, "running");
        this.scheduleDrain = Objects.requireNonNull(scheduleDrain, "scheduleDrain");
    }

    ExecutorAdmissionAttempt<C> tryAcquire(C connection, int retainedBytes) {
        Objects.requireNonNull(connection, "connection");
        if (retainedBytes < 0) {
            throw new IllegalArgumentException("retainedBytes must be >= 0");
        }

        ExecutionConnectionContext context = connection.context();
        if (!running.getAsBoolean()) {
            context.recordCommandRejected();
            submitRejectedNotRunning.increment();
            return new ExecutorAdmissionAttempt.Rejected<>(CommandExecutor.SubmitRejectReason.NOT_RUNNING);
        }
        if (context.isClosing()) {
            context.recordCommandRejected();
            submitRejectedClosing.increment();
            return new ExecutorAdmissionAttempt.Rejected<>(CommandExecutor.SubmitRejectReason.CONNECTION_CLOSING);
        }
        if (!backlogBudget.canEverReserveQueuedBytes(retainedBytes)) {
            context.recordCommandRejected();
            submitRejectedRequestTooLarge.increment();
            return new ExecutorAdmissionAttempt.Rejected<>(CommandExecutor.SubmitRejectReason.REQUEST_TOO_LARGE);
        }

        ExecutorAdmissionAttempt.BlockReason blocked = backlogBudget.tryReserve(retainedBytes);
        if (blocked != null) {
            backpressureController.disableAutoRead(connection);
            if (blocked == ExecutorAdmissionAttempt.BlockReason.QUEUE_SLOTS) {
                submitRejectedQueueFull.increment();
            } else {
                submitRejectedBytesBudget.increment();
            }
            return new ExecutorAdmissionAttempt.Unavailable<>(blocked);
        }
        if (backlogBudget.isGlobalBackpressureHigh()) {
            backpressureController.disableAutoRead(connection);
        }
        return new ExecutorAdmissionAttempt.Acquired<>(new ExecutorAdmission<>(this, connection, retainedBytes));
    }

    // OPEN -> PUBLISHED 后，request 和 reply 的唯一所有者就是此方法；失败不能把它们重新交给调用方。
    void publish(
            ExecutorAdmission<C> admission,
            ExecutionRequest request,
            ExecutionReply reply
    ) {
        C connection = admission.connection();
        int retainedBytes = admission.retainedBytes();
        CommandExecutorTask<C> task = new CommandExecutorTask<>(connection, request, retainedBytes, reply);
        boolean recorded = false;
        boolean offered = false;
        try {
            connection.context().recordCommandEnqueued(retainedBytes);
            recorded = true;
            if (!taskQueue.offer(connection, task)) {
                throw new IllegalStateException("reserved executor admission could not be published");
            }
            offered = true;
            ExecutionConnectionContext context = connection.context();
            if (context.pending() >= backpressureHighWatermark) {
                backpressureController.disableAutoRead(connection);
            }
            if (backpressureBytesHighWatermark > 0 && context.pendingBytes() >= backpressureBytesHighWatermark) {
                backpressureController.disableAutoRead(connection);
            }
            if (backlogBudget.isGlobalBackpressureHigh()) {
                backpressureController.disableAutoRead(connection);
            }
            submitAccepted.increment();
            scheduleDrain.run();
            return;
        } catch (Throwable ignored) {
            // 若 owner 已取走任务，所有权留在正常终止路径；只有确认移除后才能在这里回收。
            Boolean removed = removeAfterPublishFailure(connection, task, offered);
            if (removed == null || !removed) {
                terminateAfterPublishFailure(connection);
                return;
            }
            if (recorded) {
                try {
                    connection.context().rollbackCommandEnqueued(retainedBytes);
                } catch (Throwable ignoredRollback) {
                }
            }
            submitRejectedOfferFailed.increment();
            closeAcceptedTask(task);
            terminateAfterPublishFailure(connection);
        }
    }

    void releaseUnpublished(ExecutorAdmission<C> admission) {
        backlogBudget.release(admission.retainedBytes());
    }

    private Boolean removeAfterPublishFailure(
            C connection,
            CommandExecutorTask<C> task,
            boolean offered
    ) {
        if (!offered) {
            return Boolean.TRUE;
        }
        try {
            return taskQueue.remove(connection, task);
        } catch (Throwable ignored) {
            return null;
        }
    }

    private void closeAcceptedTask(CommandExecutorTask<C> task) {
        try {
            task.request.close();
        } catch (Throwable ignored) {
        }
        try {
            task.reply.cancel();
        } catch (Throwable ignored) {
        }
        try {
            backlogBudget.release(task.retainedBytes);
        } catch (Throwable ignored) {
        }
    }

    private void terminateAfterPublishFailure(C connection) {
        try {
            connection.markClosing();
        } catch (Throwable ignored) {
        }
        try {
            ioAdapter.closeConnection(connection);
        } catch (Throwable ignored) {
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

}
