package yier.bubu.redis.execution.api;

import yier.bubu.redis.bytes.BytesSource;
import yier.bubu.redis.common.command.ImmutableCommandRecord;

/**
 * Primary protocol-agnostic request contract for command execution.
 * <p>
 * Requests expose argv-style access where {@code argv[0]} is the command name and later elements are arguments.
 * Arguments may be {@code null} to represent a null bulk string.
 */
public interface ExecutionRequest extends ImmutableCommandRecord {
    int argc();

    boolean isNull(int index);

    int len(int index);

    byte byteAt(int index, int offset);

    void copyToByteArray(int index, byte[] dst, int dstOff);

    @Override
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

    /**
     * 当前请求视图存活期间持有的完整准入内存额度。
     */
    default long admittedMemoryBytes() {
        return retainedBytes();
    }

    @Override
    default long retainedMemoryBytes() {
        return admittedMemoryBytes();
    }

    /**
     * 创建一个由新所有者关闭一次的独立保留视图。
     */
    @Override
    default ExecutionRequest retain() {
        return ByteArrayExecutionRequest.copyOf(this);
    }

    @Override
    void close();
}
