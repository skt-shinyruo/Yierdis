package yier.bubu.redis.protocol.resp.netty;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 连接的可关闭事件循环回调与脱离传输对象的计费身份之间的边界。
 */
public final class InboundConnectionMemory implements AutoCloseable {
    private static final long NO_GRANTED_RESERVATION = -1L;

    private final ConnectionMemoryAccount account;
    private final AtomicLong grantedReservationBytes = new AtomicLong(NO_GRANTED_RESERVATION);
    private volatile Executor resumeExecutor;
    private volatile Runnable resumeCallback;
    private volatile InboundMemoryBudget budget;

    public InboundConnectionMemory(long hardLimitBytes, Executor resumeExecutor, Runnable resumeCallback) {
        this.account = new ConnectionMemoryAccount(hardLimitBytes);
        this.resumeExecutor = Objects.requireNonNull(resumeExecutor, "resumeExecutor");
        this.resumeCallback = Objects.requireNonNull(resumeCallback, "resumeCallback");
    }

    ConnectionMemoryAccount account() {
        return account;
    }

    public boolean closed() {
        return account.closed();
    }

    public void setResumeCallback(Executor executor, Runnable callback) {
        if (account.closed()) {
            return;
        }
        resumeExecutor = Objects.requireNonNull(executor, "executor");
        resumeCallback = Objects.requireNonNull(callback, "callback");
    }

    @Override
    public void close() {
        InboundMemoryBudget currentBudget = budget;
        if (currentBudget != null) {
            currentBudget.closeConnection(this);
        } else {
            account.markClosed();
        }
        resumeCallback = null;
        resumeExecutor = null;
    }

    void attach(InboundMemoryBudget candidate) {
        InboundMemoryBudget current = budget;
        if (current != null && current != candidate) {
            throw new IllegalStateException("connection memory is already attached to another budget");
        }
        budget = candidate;
    }

    boolean scheduleResume() {
        Executor executor = resumeExecutor;
        Runnable callback = resumeCallback;
        if (account.closed() || executor == null || callback == null) {
            return false;
        }
        try {
            executor.execute(callback);
            return true;
        } catch (RuntimeException ignored) {
            return false;
        }
    }

    void markGrantedReservation(long bytes) {
        if (bytes < 0L || !grantedReservationBytes.compareAndSet(NO_GRANTED_RESERVATION, bytes)) {
            throw new IllegalStateException("connection already has an undelivered reservation");
        }
    }

    boolean claimGrantedReservation(long bytes) {
        return bytes >= 0L && grantedReservationBytes.compareAndSet(bytes, NO_GRANTED_RESERVATION);
    }

    long clearGrantedReservation() {
        return grantedReservationBytes.getAndSet(NO_GRANTED_RESERVATION);
    }
}
