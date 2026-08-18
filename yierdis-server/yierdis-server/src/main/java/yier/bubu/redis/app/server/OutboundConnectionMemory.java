package yier.bubu.redis.app.server;

import java.util.Objects;
import java.util.Optional;

/**
 * 一个连接的出站回复账户，不持有 Channel 等传输对象。
 */
public final class OutboundConnectionMemory implements AutoCloseable {
    private final OutboundMemoryBudget budget;
    private final long capacityBytes;

    private volatile long reservedBytes;
    private volatile long allocatedBytes;
    private volatile long activeSlots;
    private volatile boolean closed;

    OutboundConnectionMemory(OutboundMemoryBudget budget, long capacityBytes) {
        this.budget = Objects.requireNonNull(budget, "budget");
        this.capacityBytes = capacityBytes;
    }

    public long capacityBytes() {
        return capacityBytes;
    }

    public long reservedBytes() {
        return reservedBytes;
    }

    public long allocatedBytes() {
        return allocatedBytes;
    }

    public long activeSlots() {
        return activeSlots;
    }

    public boolean closed() {
        return closed;
    }

    public Optional<OutboundMemoryLease> reserve(long bytes, long singleReplyLimitBytes) {
        return budget.reserve(this, bytes, singleReplyLimitBytes);
    }

    public boolean awaitCapacity(long bytes, long singleReplyLimitBytes, Runnable callback) {
        return budget.awaitCapacity(this, bytes, singleReplyLimitBytes, callback);
    }

    public void cancelWaiter() {
        budget.cancelWaiter(this);
    }

    @Override
    public void close() {
        budget.closeConnection(this);
    }

    void addReservation(long bytes) {
        reservedBytes += bytes;
        activeSlots++;
    }

    void extendReservation(long bytes) {
        reservedBytes += bytes;
    }

    void releaseReservation(long bytes) {
        if (bytes > reservedBytes || activeSlots <= 0L) {
            throw new IllegalStateException("outbound connection reservation underflow");
        }
        reservedBytes -= bytes;
        activeSlots--;
    }

    void addAllocated(long bytes) {
        allocatedBytes += bytes;
    }

    void releaseAllocated(long bytes) {
        if (bytes > allocatedBytes) {
            throw new IllegalStateException("outbound connection allocation underflow");
        }
        allocatedBytes -= bytes;
    }

    void markClosed() {
        closed = true;
    }

    OutboundMemoryBudget budget() {
        return budget;
    }
}
