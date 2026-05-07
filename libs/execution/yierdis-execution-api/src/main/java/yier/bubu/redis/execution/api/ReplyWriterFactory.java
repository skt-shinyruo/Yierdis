package yier.bubu.redis.execution.api;

import yier.bubu.redis.bytes.BytesSink;

/**
 * Factory for creating protocol-specific {@link ReplyWriter} instances.
 * <p>
 * This indirection allows the server/executor to remain decoupled from a concrete wire protocol implementation.
 */
@FunctionalInterface
public interface ReplyWriterFactory {
    ReplyWriter newWriter(BytesSink out);
}

