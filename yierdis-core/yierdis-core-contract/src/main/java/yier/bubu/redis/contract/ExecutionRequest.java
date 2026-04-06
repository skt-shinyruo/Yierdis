package yier.bubu.redis.contract;

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
