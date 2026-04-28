package yier.bubu.redis.engine;

import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ExecutionRequest;

/**
 * Engine facade for command execution and owner-thread maintenance.
 *
 * This phase keeps the existing CommandContext shape so executor-core can keep
 * using its transport-neutral CommandExecutionEngine seam. Later phases will
 * move business session state behind an engine-owned session type.
 */
public interface YierdisEngine extends AutoCloseable {
    void execute(ExecutionRequest request, CommandContext context);

    void maintenanceTick();

    @Override
    default void close() {
    }
}
