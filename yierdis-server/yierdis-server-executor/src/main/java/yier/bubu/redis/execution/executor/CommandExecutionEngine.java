package yier.bubu.redis.execution.executor;

import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReplyWriter;
import yier.bubu.redis.execution.api.Session;

/**
 * Transport-neutral command execution boundary used by executor-core.
 * Executor-core supplies scheduling-neutral inputs only; command contexts are
 * created inside the engine/command layer.
 */
@FunctionalInterface
public interface CommandExecutionEngine {
    void execute(Session session, ExecutionRequest request, ReplyWriter out);
}
