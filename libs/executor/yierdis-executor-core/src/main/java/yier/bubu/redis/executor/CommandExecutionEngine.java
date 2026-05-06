package yier.bubu.redis.executor;

import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.contract.Session;

/**
 * Transport-neutral command execution boundary used by executor-core.
 * Executor-core supplies scheduling-neutral inputs only; command contexts are
 * created inside the engine/command layer.
 */
@FunctionalInterface
public interface CommandExecutionEngine {
    void execute(Session session, ExecutionRequest request, ReplyWriter out);
}
