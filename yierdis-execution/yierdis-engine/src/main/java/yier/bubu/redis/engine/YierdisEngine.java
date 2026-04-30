package yier.bubu.redis.engine;

import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.contract.Session;

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
