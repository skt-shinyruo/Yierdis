package yier.bubu.redis.command.kernel;

import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;

public final class YierdisFastCommandProcessor {
    private final CommandDispatcher dispatcher;

    public YierdisFastCommandProcessor(CommandRegistry registry) {
        dispatcher = new CommandDispatcher(registry);
    }

    public PreparedCommand prepare(ExecutionRequest request, CommandPreparationContext context) {
        return dispatcher.prepare(context.session(), request);
    }

    PreparedCommand prepareQueued(ExecutionRequest request, CommandPreparationContext context) {
        return dispatcher.prepareReplay(context.session(), request);
    }
}
