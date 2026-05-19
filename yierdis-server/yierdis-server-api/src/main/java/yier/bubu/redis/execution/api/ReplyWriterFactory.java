package yier.bubu.redis.execution.api;

import yier.bubu.redis.bytes.BytesSink;

import java.util.Objects;

/**
 * Factory for creating wire-specific {@link ReplyWriter} instances that encode the Redis reply model.
 * <p>
 * This indirection allows the server/executor to remain decoupled from a concrete wire protocol implementation.
 */
@FunctionalInterface
public interface ReplyWriterFactory {
    ReplyWriter newWriter(BytesSink out);

    default ReplyWriter newWriter(Session session, BytesSink out) {
        return newWriter(Objects.requireNonNull(out, "out"));
    }
}
