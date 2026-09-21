package yier.bubu.redis.command.defaults;

import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.SlowCommandLimits;
import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.command.defaults.connection.CoreConnectionCommands;
import yier.bubu.redis.command.defaults.hash.HashCommands;
import yier.bubu.redis.command.defaults.hll.HllCommands;
import yier.bubu.redis.command.defaults.keyspace.KeyCommands;
import yier.bubu.redis.command.defaults.list.ListCommands;
import yier.bubu.redis.command.defaults.set.SetCommands;
import yier.bubu.redis.command.defaults.string.StringCommands;
import yier.bubu.redis.command.defaults.zset.ZSetCommands;
import java.util.Objects;

/**
 * Factory for the default transport-neutral command bundle.
 */
public final class DefaultCommandModules {
    private DefaultCommandModules() {
    }

    public static CommandModule create(
            YierdisDbRouter dbRouter,
            ServerInfoProvider infoProvider,
            SlowCommandLimits slowCommandLimits
    ) {
        Objects.requireNonNull(dbRouter, "dbRouter");
        Objects.requireNonNull(slowCommandLimits, "slowCommandLimits");
        return registration -> {
            CommandSupport support = new CommandSupport(dbRouter, infoProvider, slowCommandLimits);
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
}
