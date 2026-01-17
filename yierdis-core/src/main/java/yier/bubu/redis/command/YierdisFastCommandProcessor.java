package yier.bubu.redis.command;

import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapOutOfMemoryException;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespWriter;

import java.util.Objects;

/**
 * A server-side command processor optimized for low allocation.
 * <p>
 * It executes commands and writes RESP replies directly via {@link RespWriter}.
 */
public final class YierdisFastCommandProcessor {
    private static final String NULL_BULK_STRING_ERR = "ERR Protocol error: null bulk string";

    private final CommandRegistry registry;

    public YierdisFastCommandProcessor(YierdisDb db) {
        this(db, null);
    }

    public YierdisFastCommandProcessor(YierdisDb db, ServerInfoProvider infoProvider) {
        Objects.requireNonNull(db, "db");
        CommandSupport support = new CommandSupport(db, infoProvider);
        CommandRegistry registry = new CommandRegistry();
        new ServerCommands(support).register(registry);
        new KeyCommands(support).register(registry);
        new StringCommands(support).register(registry);
        new HllCommands(support).register(registry);
        new ListCommands(support).register(registry);
        new HashCommands(support).register(registry);
        new SetCommands(support).register(registry);
        new ZSetCommands(support).register(registry);
        this.registry = registry;
    }

    public void execute(RespCommand cmd, RespWriter out) {
        int argc = cmd.argc();
        if (argc <= 0) {
            out.error("ERR empty command");
            return;
        }
        if (cmd.isNull(0) || cmd.len(0) == 0) {
            out.error("ERR empty command");
            return;
        }

        // Reject null bulk strings early to avoid NPEs deeper in the DB and data structures.
        // We only allow a null bulk string for PING/ECHO's single message argument (argv[1]).
        boolean allowNullMessage = CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "PING")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "ECHO");
        for (int i = 1; i < argc; i++) {
            if (!cmd.isNull(i)) {
                continue;
            }
            if (allowNullMessage && argc == 2 && i == 1) {
                continue;
            }
            out.error(NULL_BULK_STRING_ERR);
            return;
        }

        try {
            CommandRegistry.CommandHandler handler = registry.find(cmd);
            if (handler == null) {
                out.error("ERR unknown command");
                return;
            }
            handler.execute(cmd, out);
        } catch (YierdisDb.WrongTypeException e) {
            out.error(e.getMessage());
        } catch (YierdisDb.YierdisCommandException e) {
            out.error(e.getMessage());
        } catch (YierdisOffHeapOutOfMemoryException e) {
            out.error("OOM off-heap memory limit exceeded");
        } catch (IllegalArgumentException e) {
            out.error("ERR " + e.getMessage());
        }
    }
}
