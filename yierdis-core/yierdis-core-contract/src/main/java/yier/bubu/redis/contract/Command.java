package yier.bubu.redis.contract;

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
public interface Command extends ExecutionRequest {
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
}
