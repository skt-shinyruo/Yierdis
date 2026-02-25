package yier.bubu.redis.protocol;

import yier.bubu.redis.bytes.BytesSlice;

/**
 * A narrow sink for streaming bulk-string values into a reply.
 * <p>
 * This interface exists to keep the storage/value layers decoupled from protocol-level "reply shape" concerns
 * (array/map headers, errors, etc.), while still allowing low-allocation streaming of bulk string values.
 */
public interface ReplySink {
    /**
     * Writes a bulk string value.
     * <p>
     * Passing {@code null} represents a "null bulk string" semantics.
     */
    void bulkString(byte[] data);

    /**
     * Writes a bulk string value from a byte array slice.
     */
    void bulkString(byte[] data, int off, int len);

    /**
     * Writes a bulk string value from a {@link BytesSlice}.
     */
    void bulkString(BytesSlice slice);

    /**
     * Writes a bulk string value representing the given long as an ASCII decimal string.
     */
    void bulkStringLongAscii(long value);

    /**
     * Convenience helper for writing a null bulk string.
     */
    default void bulkStringNull() {
        bulkString((byte[]) null);
    }
}
