package yier.bubu.redis.execution.api;

import yier.bubu.redis.bytes.BytesSink;

/**
 * Factory for creating wire-specific {@link RedisReplyWriter} instances that encode the Redis reply model.
 * <p>
 * This indirection allows the server/executor to remain decoupled from a concrete wire protocol implementation.
 */
@FunctionalInterface
public interface RedisReplyWriterFactory {
    RedisReplyWriter newWriter(CommandSession session, BytesSink out);
}
