package yier.bubu.redis.command;

import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapOutOfMemoryException;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespServerSession;
import yier.bubu.redis.protocol.RespTransactionState;
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
        this(singleDbRouter(db), infoProvider);
    }

    public YierdisFastCommandProcessor(YierdisDbRouter dbRouter, ServerInfoProvider infoProvider) {
        Objects.requireNonNull(dbRouter, "dbRouter");
        CommandSupport support = new CommandSupport(dbRouter, infoProvider);
        CommandRegistry registry = new CommandRegistry();
        new TransactionCommands(support, this).register(registry);
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

        RespTransactionState tx = null;
        if (out.session() instanceof RespServerSession s) {
            tx = s.transaction();
        }
        if (tx != null && tx.active()) {
            boolean isMulti = CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "MULTI");
            boolean isExec = CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "EXEC");
            boolean isDiscard = CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "DISCARD");
            if (!isMulti && !isExec && !isDiscard) {
                tx.enqueue(copyArgv(cmd));
                out.simpleString("QUEUED");
                return;
            }
        }

        try {
            CommandRegistry.CommandHandler handler = registry.find(cmd);
            if (handler == null) {
                out.error(unknownCommandMessage(cmd));
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

    private static byte[][] copyArgv(RespCommand cmd) {
        int argc = cmd == null ? 0 : cmd.argc();
        byte[][] argv = new byte[argc][];
        for (int i = 0; i < argc; i++) {
            argv[i] = cmd.toByteArray(i);
        }
        return argv;
    }

    private static String unknownCommandMessage(RespCommand cmd) {
        if (cmd == null || cmd.argc() <= 0 || cmd.isNull(0) || cmd.len(0) <= 0) {
            return "ERR unknown command";
        }
        int len = cmd.len(0);
        int printable = 0;
        for (int i = 0; i < len; i++) {
            int b = cmd.byteAt(0, i) & 0xFF;
            if (b >= 0x20 && b <= 0x7E && b != '\'' && b != '\\') {
                printable++;
            }
        }
        if (printable == len && len <= 64) {
            byte[] name = cmd.toByteArray(0);
            String s = name == null ? "" : new String(name, java.nio.charset.StandardCharsets.US_ASCII);
            return "ERR unknown command '" + s + "'";
        }
        return "ERR unknown command";
    }

    private static YierdisDbRouter singleDbRouter(YierdisDb db) {
        YierdisDb fixed = Objects.requireNonNull(db, "db");
        return new YierdisDbRouter() {
            @Override
            public YierdisDb dbFor(RespWriter out) {
                return fixed;
            }

            @Override
            public int databases() {
                return 1;
            }
        };
    }
}
