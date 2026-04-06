package yier.bubu.redis;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.executor.ExecutorBacklogBudget;
import yier.bubu.redis.executor.ExecutorBackpressureController;
import yier.bubu.redis.executor.ExecutorTaskQueue;

import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;

final class NettyCommandSubmitter {
    private final Logger log;
    private final ExecutorBacklogBudget backlogBudget;
    private final ExecutorTaskQueue<Channel, NettyExecutorTask> taskQueue;
    private final ExecutorBackpressureController<Channel> backpressureController;
    private final int backpressureHighWatermark;
    private final long backpressureBytesHighWatermark;
    private final BooleanSupplier running;
    private final Runnable scheduleDrain;
    private final LongAdder submitAccepted;
    private final LongAdder submitRejectedNotRunning;
    private final LongAdder submitRejectedQueueFull;
    private final LongAdder submitRejectedBytesBudget;
    private final LongAdder submitRejectedOfferFailed;

    NettyCommandSubmitter(
            Logger log,
            ExecutorBacklogBudget backlogBudget,
            ExecutorTaskQueue<Channel, NettyExecutorTask> taskQueue,
            ExecutorBackpressureController<Channel> backpressureController,
            int backpressureHighWatermark,
            long backpressureBytesHighWatermark,
            BooleanSupplier running,
            Runnable scheduleDrain,
            LongAdder submitAccepted,
            LongAdder submitRejectedNotRunning,
            LongAdder submitRejectedQueueFull,
            LongAdder submitRejectedBytesBudget,
            LongAdder submitRejectedOfferFailed
    ) {
        this.log = Objects.requireNonNull(log, "log");
        this.backlogBudget = Objects.requireNonNull(backlogBudget, "backlogBudget");
        this.taskQueue = Objects.requireNonNull(taskQueue, "taskQueue");
        this.backpressureController = Objects.requireNonNull(backpressureController, "backpressureController");
        this.backpressureHighWatermark = backpressureHighWatermark;
        this.backpressureBytesHighWatermark = backpressureBytesHighWatermark;
        this.running = Objects.requireNonNull(running, "running");
        this.scheduleDrain = Objects.requireNonNull(scheduleDrain, "scheduleDrain");
        this.submitAccepted = Objects.requireNonNull(submitAccepted, "submitAccepted");
        this.submitRejectedNotRunning = Objects.requireNonNull(submitRejectedNotRunning, "submitRejectedNotRunning");
        this.submitRejectedQueueFull = Objects.requireNonNull(submitRejectedQueueFull, "submitRejectedQueueFull");
        this.submitRejectedBytesBudget = Objects.requireNonNull(submitRejectedBytesBudget, "submitRejectedBytesBudget");
        this.submitRejectedOfferFailed = Objects.requireNonNull(submitRejectedOfferFailed, "submitRejectedOfferFailed");
    }

    NettyCommandExecutor.SubmitRejectReason trySubmitWithReason(ChannelHandlerContext ctx, ExecutionRequest request) {
        Objects.requireNonNull(ctx, "ctx");
        Objects.requireNonNull(request, "request");
        Channel ch = ctx.channel();
        ServerConnectionContext connection = ServerConnectionContext.getOrCreate(ch);
        if (!running.getAsBoolean()) {
            submitRejectedNotRunning.increment();
            connection.recordCommandRejected();
            return NettyCommandExecutor.SubmitRejectReason.NOT_RUNNING;
        }

        int pending = connection.pending();
        long pendingBytes = connection.pendingBytes();
        if (backlogBudget.isGlobalBackpressureHigh()) {
            backpressureController.disableAutoRead(ch);
        }
        if (pending >= backpressureHighWatermark) {
            backpressureController.disableAutoRead(ch);
        }
        if (backpressureBytesHighWatermark > 0 && pendingBytes >= backpressureBytesHighWatermark) {
            backpressureController.disableAutoRead(ch);
        }

        boolean reservedSlot = false;
        if (!backlogBudget.tryReserveSlot()) {
            // Global queue full: apply backpressure to avoid busy storms.
            backpressureController.disableAutoRead(ch);
            submitRejectedQueueFull.increment();
            connection.recordCommandRejected();
            return NettyCommandExecutor.SubmitRejectReason.QUEUE_FULL;
        }
        reservedSlot = true;

        int retainedBytes = 0;
        boolean reservedBytes = false;
        try {
            retainedBytes = NettyCommandExecutionSupport.safeRetainedBytes(request);
            if (!backlogBudget.tryReserveQueuedBytes(retainedBytes)) {
                if (reservedSlot) {
                    backlogBudget.releaseSlot();
                }
                backpressureController.disableAutoRead(ch);
                submitRejectedBytesBudget.increment();
                connection.recordCommandRejected();
                return NettyCommandExecutor.SubmitRejectReason.BYTES_BUDGET;
            }
            reservedBytes = true;

            boolean accepted = taskQueue.offer(ch, NettyExecutorTask.command(ctx, request, retainedBytes));
            if (!accepted) {
                backlogBudget.releaseQueuedBytes(retainedBytes);
                if (reservedSlot) {
                    backlogBudget.releaseSlot();
                }
                backpressureController.disableAutoRead(ch);
                submitRejectedOfferFailed.increment();
                connection.recordCommandRejected();
                return NettyCommandExecutor.SubmitRejectReason.OFFER_FAILED;
            }

            submitAccepted.increment();
            connection.recordCommandEnqueued(retainedBytes);
            int now = connection.pending();
            if (now >= backpressureHighWatermark) {
                backpressureController.disableAutoRead(ch);
            }

            long bytesNow = connection.pendingBytes();
            if (backpressureBytesHighWatermark > 0 && bytesNow >= backpressureBytesHighWatermark) {
                backpressureController.disableAutoRead(ch);
            }
            if (backlogBudget.isGlobalBackpressureHigh()) {
                backpressureController.disableAutoRead(ch);
            }

            scheduleDrain.run();
            return null;
        } catch (Throwable t) {
            if (reservedBytes) {
                backlogBudget.releaseQueuedBytes(retainedBytes);
            }
            if (reservedSlot) {
                backlogBudget.releaseSlot();
            }
            submitRejectedOfferFailed.increment();
            connection.recordCommandRejected();
            log.debug("Failed to submit command", t);
            return NettyCommandExecutor.SubmitRejectReason.OFFER_FAILED;
        }
    }
}
