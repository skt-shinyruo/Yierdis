package yier.bubu.redis.execution.executor;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * 跟踪 executor 暂停的连接，并在串行 owner 上按全局水位恢复输入。
 * I/O 副作用通过 {@link ExecutorBackpressureIo} 隔离，因此该类型不依赖 Netty。
 */
public final class ExecutorBackpressureController<K> {
    private final SerialOwnerExecutor decisionExecutor;
    private final ExecutorBacklogBudget backlogBudget;
    private final int backpressureLowWatermark;
    private final long backpressureBytesHighWatermark;
    private final long backpressureBytesLowWatermark;
    private final ExecutorBackpressureIo<K> io;
    private final ExecutorBackpressureRuntime<K> runtime;
    private final ExecutorBackpressureObserver<K> observer;
    private final BooleanSupplier isRunning;

    private final AtomicBoolean globalRecoveryScheduled = new AtomicBoolean(false);
    private final ConcurrentHashMap<K, Boolean> keysWithAutoReadDisabled = new ConcurrentHashMap<>();

    public ExecutorBackpressureController(
            SerialOwnerExecutor decisionExecutor,
            ExecutorBacklogBudget backlogBudget,
            int backpressureLowWatermark,
            long backpressureBytesHighWatermark,
            long backpressureBytesLowWatermark,
            ExecutorBackpressureIo<K> io,
            ExecutorBackpressureRuntime<K> runtime,
            ExecutorBackpressureObserver<K> observer,
            BooleanSupplier isRunning
    ) {
        this.decisionExecutor = Objects.requireNonNull(decisionExecutor, "decisionExecutor");
        this.backlogBudget = Objects.requireNonNull(backlogBudget, "backlogBudget");
        this.backpressureLowWatermark = backpressureLowWatermark;
        this.backpressureBytesHighWatermark = backpressureBytesHighWatermark;
        this.backpressureBytesLowWatermark = backpressureBytesLowWatermark;
        this.io = Objects.requireNonNull(io, "io");
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.observer = Objects.requireNonNull(observer, "observer");
        this.isRunning = Objects.requireNonNull(isRunning, "isRunning");
    }

    public int keysAutoReadDisabledCount() {
        return keysWithAutoReadDisabled.size();
    }

    public void disableAutoRead(K key) {
        if (key == null) {
            return;
        }
        if (!runtime.markAutoReadDisabledByExecutor(key)) {
            return;
        }
        safeObserverEnter(key);
        trackAutoReadDisabled(key);
        try {
            io.disableAutoRead(key);
        } catch (Throwable ignored) {
            // ignore
        }
    }

    public void enableAutoReadIfWeDisabled(K key) {
        if (key == null) {
            return;
        }
        if (runtime.inputPausedByReply(key)) {
            return;
        }
        // If the connection is not writable, keep autoRead disabled to avoid reading more requests than we can reply.
        if (!io.isWritable(key)) {
            return;
        }
        if (!runtime.autoReadDisabledByExecutor(key)) {
            return;
        }
        if (!runtime.clearAutoReadDisabledByExecutor(key)) {
            return;
        }
        safeObserverExit(key);
        keysWithAutoReadDisabled.remove(key);
        try {
            io.enableAutoRead(key);
        } catch (Throwable ignored) {
            // ignore
        }
    }

    public void scheduleGlobalRecovery() {
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

        for (K key : keysWithAutoReadDisabled.keySet()) {
            if (key == null) {
                continue;
            }
            if (!io.isActive(key)) {
                keysWithAutoReadDisabled.remove(key);
                continue;
            }
            if (runtime.isClosing(key)) {
                continue;
            }
            if (runtime.inputPausedByReply(key)) {
                continue;
            }

            int pending = runtime.pending(key);
            long pendingBytes = runtime.pendingBytes(key);
            boolean pendingOk = pending <= backpressureLowWatermark;
            boolean bytesOk = backpressureBytesHighWatermark <= 0 || pendingBytes <= backpressureBytesLowWatermark;
            if (pendingOk && bytesOk) {
                enableAutoReadIfWeDisabled(key);
            }
        }
    }

    private void trackAutoReadDisabled(K key) {
        if (key == null) {
            return;
        }
        if (keysWithAutoReadDisabled.putIfAbsent(key, Boolean.TRUE) == null) {
            try {
                io.onClose(key, () -> keysWithAutoReadDisabled.remove(key));
            } catch (Throwable ignored) {
                // ignore
            }
        }
    }

    private void safeObserverEnter(K key) {
        try {
            observer.onEnter(key);
        } catch (Throwable ignored) {
            // ignore
        }
    }

    private void safeObserverExit(K key) {
        try {
            observer.onExit(key);
        } catch (Throwable ignored) {
            // ignore
        }
    }
}
