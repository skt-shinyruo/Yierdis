package yier.bubu.redis.protocol.resp.netty;

import java.util.Objects;

/**
 * 不持有 Netty 对象的连接内存计费身份，可在最后一个请求视图关闭时继续归还额度。
 */
public final class ConnectionMemoryAccount {
    private final String id;
    private final long hardLimitBytes;
    private volatile long reservedBytes;
    private volatile boolean closed;

    ConnectionMemoryAccount(String id, long hardLimitBytes) {
        this.id = Objects.requireNonNull(id, "id");
        if (hardLimitBytes < 0L) {
            throw new IllegalArgumentException("hardLimitBytes must be non-negative");
        }
        this.hardLimitBytes = hardLimitBytes;
    }

    public String id() {
        return id;
    }

    public long hardLimitBytes() {
        return hardLimitBytes;
    }

    public long reservedBytes() {
        return reservedBytes;
    }

    public boolean closed() {
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
