package yier.bubu.redis.execution.engine;

import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;

import java.util.Objects;

/**
 * Default engine facade backed by the current command processor.
 */
public final class DefaultYierdisEngine implements YierdisEngine {
    private final YierdisFastCommandProcessor commandProcessor;
    private final Runnable maintenanceTick;

    public DefaultYierdisEngine(
            YierdisFastCommandProcessor commandProcessor,
            Runnable maintenanceTick
    ) {
        this.commandProcessor = Objects.requireNonNull(commandProcessor, "commandProcessor");
        this.maintenanceTick = Objects.requireNonNull(maintenanceTick, "maintenanceTick");
    }

    @Override
    public void execute(CommandSession session, ExecutionRequest request, RedisReplyWriter reply) {
        commandProcessor.execute(
                request,
                new CommandContext(
                        session,
                        reply,
                        MutationContext.of(request)
                )
        );
    }

    @Override
    public void maintenanceTick() {
        maintenanceTick.run();
    }
}
