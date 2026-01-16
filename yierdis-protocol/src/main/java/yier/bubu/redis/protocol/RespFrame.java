package yier.bubu.redis.protocol;

import yier.bubu.redis.db.offheap.api.YierdisBytesSource;

/**
 * Backing bytes for a decoded RESP command.
 * <p>
 * This abstraction allows the command decoder to be implemented on top of different I/O stacks
 * (e.g. Netty {@code ByteBuf}, heap byte arrays, etc.) while keeping {@link RespCommand} Netty-free.
 */
public interface RespFrame extends YierdisBytesSource, AutoCloseable {
    /**
     * Returns the total length in bytes of this frame.
     * <p>
     * This is used for backpressure/accounting (e.g. queued command retained bytes) and must be stable for the
     * lifetime of the frame.
     */
    int length();

    /**
     * Returns an estimate of bytes retained (kept alive) by holding onto this frame.
     * <p>
     * Default implementation falls back to {@link #length()}, which is a stable lower bound.
     * Implementations may override to provide a more accurate estimate for backpressure/budgeting.
     * The returned value MUST be stable for the lifetime of the frame.
     */
    default int retainedBytes() {
        return length();
    }

    @Override
    void close();
}
