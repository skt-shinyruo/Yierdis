package yier.bubu.redis.execution.api;

/**
 * Compatibility name for the Redis command reply writer used by the command layer.
 * <p>
 * New code that wants to describe the reply model precisely should refer to {@link RedisReplyWriter}.
 * This type remains the stable execution boundary for existing command, engine, executor, and
 * protocol adapter APIs.
 */
public interface ReplyWriter extends RedisReplyWriter {
}
