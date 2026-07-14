package yier.bubu.redis.execution.api;

import java.util.Objects;

/**
 * Redis command reply model used by the command layer.
 * <p>
 * This interface is not a generic protocol writer. It exposes the Redis reply shapes that command
 * handlers produce, including RESP2-compatible scalars and the RESP3/Redis aggregate forms used by
 * HELLO, INFO/STATS, and collection replies. Protocol implementations encode these shapes into the
 * active wire format without making command handlers depend on protocol packages.
 */
public interface RedisReplyWriter extends ReplySink {
    @FunctionalInterface
    interface MeasuredReplyVisitor {
        void writeTo(RedisReplyWriter out);
    }

    /**
     * 在生成回复字节前请求容量；不支持容量管理的 detached writer 保持兼容性 no-op。
     */
    default void requireReply(ReplyPlan plan) {
    }

    /**
     * 将异步回复完成前仍需保留的来源转交给 writer；同步 writer 会在此处关闭它。
     */
    default void transferReplyOwnership(AutoCloseable resource) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (RuntimeException | Error failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IllegalStateException("reply source close failed", failure);
        }
    }

    default void writeMeasuredBulkStringArray(
            int count,
            long encodedElementBytes,
            long retainedSourceBytes,
            AutoCloseable source,
            MeasuredReplyVisitor visitor
    ) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(visitor, "visitor");
        boolean ownershipTransferred = false;
        try {
            if (count < 0) {
                throw new IllegalArgumentException("measured array count must be non-negative");
            }
            requireReply(ReplyPlans.bulkStringArray(count, encodedElementBytes, retainedSourceBytes));
            arrayHeader(count);
            visitor.writeTo(this);
            transferReplyOwnership(source);
            ownershipTransferred = true;
        } finally {
            if (!ownershipTransferred) {
                closeReplySource(source);
            }
        }
    }

    default void writeMeasuredBulkStringMap(
            int pairCount,
            long encodedElementBytes,
            long retainedSourceBytes,
            AutoCloseable source,
            MeasuredReplyVisitor visitor
    ) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(visitor, "visitor");
        boolean ownershipTransferred = false;
        try {
            if (pairCount < 0 || pairCount > Integer.MAX_VALUE / 2) {
                throw new IllegalArgumentException("measured map pair count cannot be represented as a RESP2 array: " + pairCount);
            }
            requireReply(ReplyPlans.bulkStringArray(pairCount * 2, encodedElementBytes, retainedSourceBytes));
            mapHeader(pairCount);
            visitor.writeTo(this);
            transferReplyOwnership(source);
            ownershipTransferred = true;
        } finally {
            if (!ownershipTransferred) {
                closeReplySource(source);
            }
        }
    }

    private static void closeReplySource(AutoCloseable source) {
        try {
            source.close();
        } catch (RuntimeException | Error failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IllegalStateException("reply source close failed", failure);
        }
    }

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

    void emptyArray();

    void mapHeader(int pairs);

    void setHeader(int count);

    void pushHeader(int count);

    void attributeHeader(int pairs);
}
