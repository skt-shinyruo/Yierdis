package yier.bubu.redis.execution.engine;

import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.kernel.YierdisCommandProcessorOptions;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReplyWriter;
import yier.bubu.redis.execution.api.ServerSession;
import yier.bubu.redis.execution.api.Session;

import java.util.Objects;

/**
 * Default engine facade backed by the current command processor.
 */
public final class DefaultYierdisEngine implements YierdisEngine {
    private final YierdisFastCommandProcessor commandProcessor;
    private final Runnable maintenanceTick;

    public DefaultYierdisEngine(Runnable maintenanceTick, CommandModule... commandModules) {
        this(YierdisCommandProcessorOptions.DEFAULT, maintenanceTick, commandModules);
    }

    public DefaultYierdisEngine(
            YierdisCommandProcessorOptions options,
            Runnable maintenanceTick,
            CommandModule... commandModules
    ) {
        this(
                new YierdisFastCommandProcessor(options, commandModules),
                maintenanceTick
        );
    }

    DefaultYierdisEngine(YierdisFastCommandProcessor commandProcessor, Runnable maintenanceTick) {
        this.commandProcessor = Objects.requireNonNull(commandProcessor, "commandProcessor");
        this.maintenanceTick = Objects.requireNonNull(maintenanceTick, "maintenanceTick");
    }

    @Override
    public void execute(Session session, ExecutionRequest request, ReplyWriter out) {
        if (!(session instanceof ServerSession serverSession)) {
            throw new IllegalArgumentException("YierdisEngine requires ServerSession");
        }
        commandProcessor.execute(request, new CommandContext(serverSession, out));
    }

    @Override
    public void maintenanceTick() {
        maintenanceTick.run();
    }
}
