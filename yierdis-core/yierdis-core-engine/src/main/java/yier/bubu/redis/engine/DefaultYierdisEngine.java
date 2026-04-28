package yier.bubu.redis.engine;

import yier.bubu.redis.command.CommandModule;
import yier.bubu.redis.command.ServerInfoProvider;
import yier.bubu.redis.command.SlowCommandGovernor;
import yier.bubu.redis.command.YierdisDbRouter;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.contract.Session;

import java.util.Objects;

/**
 * Default engine facade backed by the current command processor.
 */
public final class DefaultYierdisEngine implements YierdisEngine {
    private final YierdisFastCommandProcessor commandProcessor;
    private final Runnable maintenanceTick;

    public DefaultYierdisEngine(
            YierdisDbRouter dbRouter,
            ServerInfoProvider infoProvider,
            SlowCommandGovernor slowGovernor,
            Runnable maintenanceTick,
            CommandModule... extraModules
    ) {
        this(
                new YierdisFastCommandProcessor(
                        Objects.requireNonNull(dbRouter, "dbRouter"),
                        infoProvider,
                        slowGovernor,
                        extraModules
                ),
                maintenanceTick
        );
    }

    DefaultYierdisEngine(YierdisFastCommandProcessor commandProcessor, Runnable maintenanceTick) {
        this.commandProcessor = Objects.requireNonNull(commandProcessor, "commandProcessor");
        this.maintenanceTick = Objects.requireNonNull(maintenanceTick, "maintenanceTick");
    }

    @Override
    public void execute(Session session, ExecutionRequest request, ReplyWriter out) {
        commandProcessor.execute(request, new CommandContext(session, out));
    }

    @Override
    public void maintenanceTick() {
        maintenanceTick.run();
    }
}
