package yier.bubu.redis.command;

import yier.bubu.redis.contract.ServerSession;
import yier.bubu.redis.ops.DbEngine;

import java.util.Objects;

/**
 * Factory for the default transport-neutral command bundle.
 */
public final class DefaultCommandModules {
    private DefaultCommandModules() {
    }

    public static CommandModule create(DbEngine engine) {
        return create(singleDbRouter(engine), null, SlowCommandGovernor.DEFAULT);
    }

    public static CommandModule create(DbEngine engine, ServerInfoProvider infoProvider) {
        return create(singleDbRouter(engine), infoProvider, SlowCommandGovernor.DEFAULT);
    }

    public static CommandModule create(YierdisDbRouter dbRouter, ServerInfoProvider infoProvider) {
        return create(dbRouter, infoProvider, SlowCommandGovernor.DEFAULT);
    }

    public static CommandModule create(
            YierdisDbRouter dbRouter,
            ServerInfoProvider infoProvider,
            SlowCommandGovernor slowGovernor
    ) {
        Objects.requireNonNull(dbRouter, "dbRouter");
        SlowCommandGovernor governor = slowGovernor == null ? SlowCommandGovernor.DEFAULT : slowGovernor;
        return registration -> {
            CommandSupport support = new CommandSupport(dbRouter, infoProvider, governor);
            new CoreConnectionCommands(support).register(registration);
            new KeyCommands(support).register(registration);
            new StringCommands(support).register(registration);
            new HllCommands(support).register(registration);
            new ListCommands(support).register(registration);
            new HashCommands(support).register(registration);
            new SetCommands(support).register(registration);
            new ZSetCommands(support).register(registration);
        };
    }

    private static YierdisDbRouter singleDbRouter(DbEngine engine) {
        DbEngine fixed = Objects.requireNonNull(engine, "engine");
        return new YierdisDbRouter() {
            @Override
            public DbEngine dbFor(ServerSession session) {
                return fixed;
            }

            @Override
            public int databases() {
                return 1;
            }
        };
    }
}
