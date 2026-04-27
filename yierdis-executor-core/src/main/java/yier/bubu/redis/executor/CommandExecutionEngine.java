package yier.bubu.redis.executor;

import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ExecutionRequest;

/**
 * Transport-neutral command execution boundary used by executor-core.
 */
@FunctionalInterface
public interface CommandExecutionEngine {
    void execute(ExecutionRequest request, CommandContext context);
}
