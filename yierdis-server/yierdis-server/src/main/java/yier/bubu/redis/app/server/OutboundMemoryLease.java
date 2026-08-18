package yier.bubu.redis.app.server;

/**
 * 一个顶层回复槽位的预留额度，可重复关闭。
 */
public final class OutboundMemoryLease implements AutoCloseable {
    private final OutboundMemoryBudget budget;
    private final OutboundConnectionMemory connection;
    private volatile long reservedBytes;

    private volatile long allocatedBytes;
    private volatile boolean closed;

    OutboundMemoryLease(OutboundMemoryBudget budget, OutboundConnectionMemory connection, long reservedBytes) {
        this.budget = budget;
        this.connection = connection;
        this.reservedBytes = reservedBytes;
    }

    public long reservedBytes() {
        return reservedBytes;
    }

    public long allocatedBytes() {
        return allocatedBytes;
    }

    public boolean closed() {
        return closed;
    }

    public boolean convertToAllocated(long bytes) {
        return budget.convertToAllocated(this, bytes);
    }

    public boolean tryReserveAdditional(long bytes, long singleReplyLimitBytes) {
        return budget.expandLease(this, bytes, singleReplyLimitBytes);
    }

    public boolean awaitAdditionalCapacity(long bytes, long singleReplyLimitBytes, Runnable callback) {
        return budget.awaitLeaseExpansion(this, bytes, singleReplyLimitBytes, callback);
    }

    void cancelAdditionalCapacityWaiter() {
        budget.cancelLeaseExpansionWaiter(this);
    }

    public void releaseAllocated(long bytes) {
        budget.releaseAllocated(this, bytes);
    }

    @Override
    public void close() {
        budget.closeLease(this);
    }

    OutboundConnectionMemory connection() {
        return connection;
    }

    void addAllocated(long bytes) {
        allocatedBytes += bytes;
    }

    void addReservedBytes(long bytes) {
        reservedBytes += bytes;
    }

    void releaseAllocatedBytes(long bytes) {
        if (bytes > allocatedBytes) {
            throw new IllegalStateException("outbound lease allocation underflow");
        }
        allocatedBytes -= bytes;
    }

    void markClosed() {
        closed = true;
    }
}
