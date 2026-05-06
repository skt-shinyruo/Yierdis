package yier.bubu.redis.contract;

/**
 * Deprecated compatibility alias for older embedders. Production code should use {@link ExecutionRequest}.
 */
@Deprecated(forRemoval = true)
public interface Command extends ExecutionRequest {
}
