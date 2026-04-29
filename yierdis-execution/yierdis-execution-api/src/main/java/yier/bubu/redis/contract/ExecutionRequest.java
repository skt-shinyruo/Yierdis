package yier.bubu.redis.contract;

import yier.bubu.redis.bytes.BytesSource;

/**
 * Primary protocol-agnostic request contract for command execution.
 * <p>
 * Requests expose argv-style access where {@code argv[0]} is the command name and later elements are arguments.
 * Arguments may be {@code null} to represent a null bulk string.
 */
public interface ExecutionRequest extends AutoCloseable {
    int argc();

    boolean isNull(int index);

    int len(int index);

    byte byteAt(int index, int offset);

    void copyToByteArray(int index, byte[] dst, int dstOff);

    byte[] toByteArray(int index);

    /**
     * Read-only argv access for hot paths that can consume immutable heap-backed bytes without copying.
     * <p>
     * Callers MUST treat the returned array as immutable. Implementations may return either a shared backing array or
     * a defensive copy when zero-copy access is unavailable.
     */
    default byte[] readOnlyByteArray(int index) {
        return toByteArray(index);
    }

    /**
     * Optional backing bytes for implementations that can expose a zero-copy frame.
     */
    default BytesSource frame() {
        return null;
    }

    /**
     * Optional argument offset within {@link #frame()}.
     */
    default int argOffset(int index) {
        return -1;
    }

    /**
     * Estimated bytes retained by keeping this request alive while queued or replayed.
     * <p>
     * The returned value MUST be stable for the lifetime of the request.
     */
    default int retainedBytes() {
        return 0;
    }

    @Override
    void close();
}
