package yier.bubu.redis.protocol;

import yier.bubu.redis.bytes.BytesSource;

/**
 * Protocol-agnostic command representation (argv-style).
 * <p>
 * The command is represented as a list of arguments (bulk strings semantics): {@code argv[0]} is the command name,
 * {@code argv[1..]} are arguments. Arguments may be {@code null} to represent a "null bulk string".
 * <p>
 * This interface is intentionally minimal and byte-oriented to support low-allocation execution and to keep the
 * command layer independent from any specific wire protocol.
 */
public interface Command extends AutoCloseable {
    int argc();

    boolean isNull(int index);

    int len(int index);

    byte byteAt(int index, int offset);

    void copyToByteArray(int index, byte[] dst, int dstOff);

    byte[] toByteArray(int index);

    /**
     * Optional backing bytes for this command.
     * <p>
     * Some implementations can expose a zero-copy backing buffer. Others may return {@code null}.
     */
    default BytesSource frame() {
        return null;
    }

    /**
     * Optional argument offset within {@link #frame()}.
     * <p>
     * Returns {@code -1} when {@link #frame()} is not available.
     */
    default int argOffset(int index) {
        return -1;
    }

    /**
     * Estimated bytes retained by keeping this command alive while queued.
     * <p>
     * The returned value MUST be stable for the lifetime of the command.
     */
    default int retainedBytes() {
        return 0;
    }

    @Override
    void close();
}
