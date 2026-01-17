package yier.bubu.redis;

// 执行器背压控制组件：负责 autoRead disable/enable、全局恢复扫描与 tracking。

import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;

final class NettyExecutorBackpressureController {
    private final EventExecutor executor;
    private final NettyExecutorBacklogBudget backlogBudget;
    private final int backpressureLowWatermark;
    private final long backpressureBytesHighWatermark;
    private final long backpressureBytesLowWatermark;
    private final LongAdder backpressureEnter;
    private final LongAdder backpressureExit;
    private final BooleanSupplier isRunning;

    private final AtomicBoolean globalRecoveryScheduled = new AtomicBoolean(false);
    private final ConcurrentHashMap<Channel, Boolean> channelsWithAutoReadDisabled = new ConcurrentHashMap<>();

    NettyExecutorBackpressureController(
            EventExecutor executor,
            NettyExecutorBacklogBudget backlogBudget,
            int backpressureLowWatermark,
            long backpressureBytesHighWatermark,
            long backpressureBytesLowWatermark,
            LongAdder backpressureEnter,
            LongAdder backpressureExit,
            BooleanSupplier isRunning
    ) {
        this.executor = Objects.requireNonNull(executor, "executor");
        this.backlogBudget = Objects.requireNonNull(backlogBudget, "backlogBudget");
        this.backpressureLowWatermark = backpressureLowWatermark;
        this.backpressureBytesHighWatermark = backpressureBytesHighWatermark;
        this.backpressureBytesLowWatermark = backpressureBytesLowWatermark;
        this.backpressureEnter = Objects.requireNonNull(backpressureEnter, "backpressureEnter");
        this.backpressureExit = Objects.requireNonNull(backpressureExit, "backpressureExit");
        this.isRunning = Objects.requireNonNull(isRunning, "isRunning");
    }

    int channelsAutoReadDisabledCount() {
        return channelsWithAutoReadDisabled.size();
    }

    void disableAutoRead(Channel ch) {
        if (ch == null) {
            return;
        }
        ServerConnectionState ctx = ServerConnectionState.getOrCreate(ch);
        if (!ctx.markAutoReadDisabledByExecutor()) {
            return;
        }
        ctx.backpressureEnterCounter().incrementAndGet();
        backpressureEnter.increment();
        trackAutoReadDisabled(ch);
        ch.eventLoop().execute(() -> {
            try {
                ch.config().setAutoRead(false);
            } catch (Throwable ignored) {
                // ignore
            }
        });
    }

    void enableAutoReadIfWeDisabled(Channel ch) {
        if (ch == null) {
            return;
        }
        ServerConnectionState ctx = ServerConnectionState.getOrCreate(ch);
        if (!ctx.autoReadDisabledByExecutor()) {
            return;
        }
        if (!ctx.clearAutoReadDisabledByExecutor()) {
            return;
        }
        ctx.backpressureExitCounter().incrementAndGet();
        backpressureExit.increment();
        channelsWithAutoReadDisabled.remove(ch);
        ch.eventLoop().execute(() -> {
            try {
                ch.config().setAutoRead(true);
            } catch (Throwable ignored) {
                // ignore
            }
        });
    }

    void scheduleGlobalRecovery() {
        if (!globalRecoveryScheduled.compareAndSet(false, true)) {
            return;
        }
        if (executor.inEventLoop()) {
            recoverGlobalAutoRead();
            return;
        }
        executor.execute(this::recoverGlobalAutoRead);
    }

    private void recoverGlobalAutoRead() {
        globalRecoveryScheduled.set(false);
        if (!isRunning.getAsBoolean()) {
            return;
        }
        if (!backlogBudget.isGlobalBackpressureCleared()) {
            return;
        }

        for (Channel ch : channelsWithAutoReadDisabled.keySet()) {
            if (ch == null) {
                continue;
            }
            if (!ch.isActive()) {
                channelsWithAutoReadDisabled.remove(ch);
                continue;
            }
            if (isChannelClosing(ch)) {
                continue;
            }

            int pending = pendingCounter(ch).get();
            long pendingBytes = pendingBytesCounter(ch).get();
            boolean pendingOk = pending <= backpressureLowWatermark;
            boolean bytesOk = backpressureBytesHighWatermark <= 0 || pendingBytes <= backpressureBytesLowWatermark;
            if (pendingOk && bytesOk) {
                enableAutoReadIfWeDisabled(ch);
            }
        }
    }

    private static AtomicInteger pendingCounter(Channel ch) {
        return ServerConnectionState.getOrCreate(ch).pendingCounter();
    }

    private static AtomicLong pendingBytesCounter(Channel ch) {
        return ServerConnectionState.getOrCreate(ch).pendingBytesCounter();
    }

    private static boolean isChannelClosing(Channel ch) {
        if (ch == null) {
            return false;
        }
        return ServerConnectionState.getOrCreate(ch).isClosing();
    }

    private void trackAutoReadDisabled(Channel ch) {
        if (ch == null) {
            return;
        }
        if (channelsWithAutoReadDisabled.putIfAbsent(ch, Boolean.TRUE) == null) {
            ch.closeFuture().addListener(ignored -> channelsWithAutoReadDisabled.remove(ch));
        }
    }
}

