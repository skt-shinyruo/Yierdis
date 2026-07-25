package yier.bubu.redis.execution.engine;

import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;

/**
 * Engine facade for command execution and owner-thread maintenance.
 * Executor and transport code pass only the session contract and request;
 * reply rendering starts after the executor reserves the prepared shape.
 */
public interface YierdisEngine extends AutoCloseable {
    PreparedCommand prepare(CommandSession session, ExecutionRequest request);

    void maintenanceTick();

    @Override
    default void close() {
    }
}
