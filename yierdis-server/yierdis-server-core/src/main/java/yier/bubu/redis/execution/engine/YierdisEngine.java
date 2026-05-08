package yier.bubu.redis.execution.engine;

import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReplyWriter;
import yier.bubu.redis.execution.api.Session;

/**
 * Engine facade for command execution and owner-thread maintenance.
 * Executor and transport code pass only the session contract, request, and
 * writer; the engine/command layer owns the command context construction.
 */
public interface YierdisEngine extends AutoCloseable {
    void execute(Session session, ExecutionRequest request, ReplyWriter out);

    void maintenanceTick();

    @Override
    default void close() {
    }
}
