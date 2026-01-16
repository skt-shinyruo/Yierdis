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

    @Override
    void close();
}
