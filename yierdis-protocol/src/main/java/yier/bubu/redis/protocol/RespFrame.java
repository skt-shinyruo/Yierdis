package yier.bubu.redis.protocol;

import yier.bubu.redis.db.offheap.api.YierdisBytesSource;

/**
 * Backing bytes for a decoded RESP command.
 * <p>
 * This abstraction allows the command decoder to be implemented on top of different I/O stacks
 * (e.g. Netty {@code ByteBuf}, heap byte arrays, etc.) while keeping {@link RespCommand} Netty-free.
 */
public interface RespFrame extends YierdisBytesSource, AutoCloseable {
    @Override
    void close();
}
