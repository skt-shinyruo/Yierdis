package yier.bubu.redis.execution.executor;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;

/**
 * 直接协调连接上下文与 I/O adapter，在串行 owner 上按全局水位恢复输入。
 */
final class ExecutorBackpressureController<C extends ExecutionConnection> {
    private final SerialOwnerExecutor decisionExecutor;
    private final ExecutorBacklogBudget backlogBudget;
    private final int backpressureLowWatermark;
    private final long backpressureBytesHighWatermark;
    private final long backpressureBytesLowWatermark;
    private final ExecutionIoAdapter<C> ioAdapter;
    private final BooleanSupplier isRunning;
    private final AtomicBoolean globalRecoveryScheduled = new AtomicBoolean(false);
    private final ConcurrentHashMap<C, Boolean> connectionsWithAutoReadDisabled = new ConcurrentHashMap<>();
    private final LongAdder backpressureEnter = new LongAdder();
    private final LongAdder backpressureExit = new LongAdder();

    ExecutorBackpressureController(
            SerialOwnerExecutor decisionExecutor,
            ExecutorBacklogBudget backlogBudget,
            int backpressureLowWatermark,
            long backpressureBytesHighWatermark,
            long backpressureBytesLowWatermark,
            ExecutionIoAdapter<C> ioAdapter,
            BooleanSupplier isRunning
    ) {
        this.decisionExecutor = Objects.requireNonNull(decisionExecutor, "decisionExecutor");
        this.backlogBudget = Objects.requireNonNull(backlogBudget, "backlogBudget");
        this.backpressureLowWatermark = backpressureLowWatermark;
        this.backpressureBytesHighWatermark = backpressureBytesHighWatermark;
        this.backpressureBytesLowWatermark = backpressureBytesLowWatermark;
        this.ioAdapter = Objects.requireNonNull(ioAdapter, "ioAdapter");
        this.isRunning = Objects.requireNonNull(isRunning, "isRunning");
    }

    int connectionsAutoReadDisabledCount() {
        return connectionsWithAutoReadDisabled.size();
    }

    long backpressureEnter() {
        return backpressureEnter.sum();
    }

    long backpressureExit() {
        return backpressureExit.sum();
    }

    void disableAutoRead(C connection) {
        if (connection == null) {
            return;
        }
        ExecutionConnectionContext context = connection.context();
        if (!context.markInputDisabledByExecutor()) {
            return;
        }
        context.recordBackpressureEnter();
        backpressureEnter.increment();
        trackAutoReadDisabled(connection);
        try {
            ioAdapter.disableInput(connection);
        } catch (Throwable ignored) {
            // 状态已记录为暂停；后续恢复仍会通过同一连接状态机重试 I/O。
        }
    }

    void enableAutoReadIfWeDisabled(C connection) {
        if (connection == null) {
            return;
        }
        ExecutionConnectionContext context = connection.context();
        if (context.inputPausedByReply()) {
            return;
        }
        // reply 容量和 transport 可写性是独立暂停原因，任一未恢复都不能重新开启输入。
        if (!ioAdapter.isWritable(connection)) {
            return;
        }
        if (!context.autoReadDisabledByExecutor()) {
            return;
        }
        if (!context.clearAutoReadDisabledByExecutor()) {
            return;
        }
        context.recordBackpressureExit();
        backpressureExit.increment();
        connectionsWithAutoReadDisabled.remove(connection);
        try {
            ioAdapter.enableInput(connection);
        } catch (Throwable ignored) {
            // context 已完成恢复；I/O 切换失败按 best-effort 处理，不重新登记背压。
        }
    }

    void scheduleGlobalRecovery() {
        if (!globalRecoveryScheduled.compareAndSet(false, true)) {
            return;
        }
        decisionExecutor.execute(this::recoverGlobalAutoRead);
    }

    private void recoverGlobalAutoRead() {
        decisionExecutor.requireOwnerThread();
        globalRecoveryScheduled.set(false);
        if (!isRunning.getAsBoolean()) {
            return;
        }
        if (!backlogBudget.isGlobalBackpressureCleared()) {
            return;
        }

        for (C connection : connectionsWithAutoReadDisabled.keySet()) {
            if (!ioAdapter.isActive(connection)) {
                connectionsWithAutoReadDisabled.remove(connection);
                continue;
            }
            ExecutionConnectionContext context = connection.context();
            if (context.isClosing()) {
                continue;
            }
            if (context.inputPausedByReply()) {
                continue;
            }

            int pending = context.pending();
            long pendingBytes = context.pendingBytes();
            boolean pendingOk = pending <= backpressureLowWatermark;
            boolean bytesOk = backpressureBytesHighWatermark <= 0 || pendingBytes <= backpressureBytesLowWatermark;
            if (pendingOk && bytesOk) {
                enableAutoReadIfWeDisabled(connection);
            }
        }
    }

    private void trackAutoReadDisabled(C connection) {
        if (connectionsWithAutoReadDisabled.putIfAbsent(connection, Boolean.TRUE) == null) {
            try {
                ioAdapter.onClose(connection, () -> connectionsWithAutoReadDisabled.remove(connection));
            } catch (Throwable ignored) {
                // 无法监听关闭时保留跟踪项，后续全局恢复会通过 isActive 清理。
            }
        }
    }
}
