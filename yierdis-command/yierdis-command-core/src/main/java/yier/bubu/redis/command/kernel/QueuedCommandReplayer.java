package yier.bubu.redis.command.kernel;

import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;

@FunctionalInterface
public interface QueuedCommandReplayer {
    void replay(ExecutionRequest request, CommandContext ctx);
}
