package yier.bubu.redis;

// Server 连接运行时状态（server 私有）：承载背压/计数/closing 等语义；跨线程写入仅限原子字段。

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

final class ServerRuntimeState {
    // --- Executor / backpressure (跨线程读写，使用原子类型) ---
    private final AtomicInteger pending = new AtomicInteger(0);
    private final AtomicLong pendingBytes = new AtomicLong(0);
    private final AtomicBoolean autoReadDisabledByExecutor = new AtomicBoolean(false);
    private final AtomicBoolean closing = new AtomicBoolean(false);

    // --- Observability（低开销容器，具体字段在 server 侧填充/使用） ---
    private final AtomicLong commandsEnqueued = new AtomicLong(0);
    private final AtomicLong commandsExecuted = new AtomicLong(0);
    private final AtomicLong commandsRejected = new AtomicLong(0);
    private final AtomicLong commandsSkippedClosing = new AtomicLong(0);
    private final AtomicLong closeAfterReply = new AtomicLong(0);
    private final AtomicLong backpressureEnter = new AtomicLong(0);
    private final AtomicLong backpressureExit = new AtomicLong(0);

    ServerRuntimeState() {
    }

    AtomicInteger pendingCounter() {
        return pending;
    }

    AtomicLong pendingBytesCounter() {
        return pendingBytes;
    }

    boolean markAutoReadDisabledByExecutor() {
        return autoReadDisabledByExecutor.compareAndSet(false, true);
    }

    boolean clearAutoReadDisabledByExecutor() {
        return autoReadDisabledByExecutor.compareAndSet(true, false);
    }

    boolean autoReadDisabledByExecutor() {
        return autoReadDisabledByExecutor.get();
    }

    boolean isClosing() {
        return closing.get();
    }

    void markClosing(ServerSessionState session) {
        closing.set(true);
        // 连接关闭时清理连接态资源，避免悬挂引用/内存驻留。
        if (session != null) {
            session.discardTransaction();
        }
    }

    AtomicLong commandsEnqueuedCounter() {
        return commandsEnqueued;
    }

    AtomicLong commandsExecutedCounter() {
        return commandsExecuted;
    }

    AtomicLong commandsRejectedCounter() {
        return commandsRejected;
    }

    AtomicLong commandsSkippedClosingCounter() {
        return commandsSkippedClosing;
    }

    AtomicLong closeAfterReplyCounter() {
        return closeAfterReply;
    }

    AtomicLong backpressureEnterCounter() {
        return backpressureEnter;
    }

    AtomicLong backpressureExitCounter() {
        return backpressureExit;
    }
}
