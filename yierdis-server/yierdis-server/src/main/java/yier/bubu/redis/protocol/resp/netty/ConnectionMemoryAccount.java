package yier.bubu.redis.protocol.resp.netty;

/**
 * 不持有 Netty 对象的连接内存计费身份，可在最后一个请求视图关闭时继续归还额度。
 */
final class ConnectionMemoryAccount {
    private final long hardLimitBytes;
    private volatile long reservedBytes;
    private volatile boolean closed;

    ConnectionMemoryAccount(long hardLimitBytes) {
        if (hardLimitBytes < 0L) {
            throw new IllegalArgumentException("hardLimitBytes must be non-negative");
        }
        this.hardLimitBytes = hardLimitBytes;
    }

    long hardLimitBytes() {
        return hardLimitBytes;
    }

    long reservedBytes() {
        return reservedBytes;
    }

    boolean closed() {
        return closed;
    }

    void addReserved(long bytes) {
        reservedBytes = InboundMemoryBudget.saturatedAdd(reservedBytes, bytes);
    }

    void releaseReserved(long bytes) {
        if (bytes > reservedBytes) {
            throw new IllegalStateException("connection memory release exceeds reservation");
        }
        reservedBytes -= bytes;
    }

    void markClosed() {
        closed = true;
    }
}
