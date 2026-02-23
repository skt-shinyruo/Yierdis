package yier.bubu.redis.protocol;

import java.util.List;

/**
 * Protocol-agnostic reply writer used by the command layer.
 * <p>
 * Implementations may encode replies in the active wire format, as long as they preserve the
 * semantic shape implied by the method calls (scalars vs aggregates, nulls, etc.).
 */
public interface ReplyWriter extends ReplySink {
    void requestCloseAfterReply();

    boolean closeAfterReplyRequested();

    /**
     * Marks the current reply as a protocol-level error.
     * <p>
     * This is distinct from command-layer {@link #error(String)} values, which are part of the command semantics and
     * may appear inside aggregates (e.g. EXEC's result array).
     */
    default void protocolError(String message) {
        error(message);
    }

    /**
     * Marks the current reply as an internal/server error.
     * <p>
     * Protocol implementations may encode this differently from command errors ({@link #error(String)}).
     */
    default void internalError(String message) {
        error(message);
    }

    // --- Scalars ---
    void simpleString(String value);

    void error(String message);

    void integer(long value);

    void booleanValue(boolean value);

    void doubleValue(double value);

    void bigNumberAscii(String value);

    void verbatimString(String format, byte[] data);

    void blobError(String message);

    // --- Aggregates ---
    void nullValue();

    void nullArray();

    void arrayHeader(int count);

    void bulkStringArray(List<byte[]> values);

    void emptyArray();

    void mapHeader(int pairs);

    void setHeader(int count);

    void pushHeader(int count);

    void attributeHeader(int pairs);
}
