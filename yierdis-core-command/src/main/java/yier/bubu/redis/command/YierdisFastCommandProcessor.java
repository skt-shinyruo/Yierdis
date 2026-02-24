package yier.bubu.redis.command;

import yier.bubu.redis.db.offheap.api.YierdisOffHeapOutOfMemoryException;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.ops.WrongTypeException;
import yier.bubu.redis.ops.YierdisCommandException;
import yier.bubu.redis.protocol.CommandContext;
import yier.bubu.redis.protocol.Command;
import yier.bubu.redis.protocol.DbIndexProvider;
import yier.bubu.redis.protocol.ReplyWriter;
import yier.bubu.redis.protocol.ServerSession;
import yier.bubu.redis.protocol.TransactionState;
import yier.bubu.redis.runtime.api.YierdisChangeEvent;
import yier.bubu.redis.runtime.api.YierdisChangeSink;

import java.util.Objects;

/**
 * A server-side command processor optimized for low allocation.
 * <p>
 * It executes commands and writes replies via {@link ReplyWriter}.
 */
public final class YierdisFastCommandProcessor {
    private static final String NULL_BULK_STRING_ERR = "ERR Protocol error: null bulk string";

    private final CommandRegistry registry;
    private final YierdisDbRouter dbRouter;
    private final YierdisChangeSink changeSink;

    public YierdisFastCommandProcessor(DbEngine engine) {
        this(engine, null, YierdisChangeSink.NOOP, null);
    }

    public YierdisFastCommandProcessor(DbEngine engine, ServerInfoProvider infoProvider) {
        this(engine, infoProvider, YierdisChangeSink.NOOP, null);
    }

    public YierdisFastCommandProcessor(DbEngine engine, ServerInfoProvider infoProvider, YierdisChangeSink changeSink) {
        this(engine, infoProvider, changeSink, null);
    }

    public YierdisFastCommandProcessor(DbEngine engine, ServerInfoProvider infoProvider, SlowCommandGovernor slowGovernor) {
        this(engine, infoProvider, YierdisChangeSink.NOOP, slowGovernor);
    }

    public YierdisFastCommandProcessor(DbEngine engine, ServerInfoProvider infoProvider, YierdisChangeSink changeSink, SlowCommandGovernor slowGovernor) {
        this(singleDbRouter(engine), infoProvider, changeSink, slowGovernor);
    }

    public YierdisFastCommandProcessor(YierdisDbRouter dbRouter, ServerInfoProvider infoProvider) {
        this(dbRouter, infoProvider, YierdisChangeSink.NOOP, null);
    }

    public YierdisFastCommandProcessor(YierdisDbRouter dbRouter, ServerInfoProvider infoProvider, YierdisChangeSink changeSink) {
        this(dbRouter, infoProvider, changeSink, null);
    }

    public YierdisFastCommandProcessor(YierdisDbRouter dbRouter, ServerInfoProvider infoProvider, SlowCommandGovernor slowGovernor) {
        this(dbRouter, infoProvider, YierdisChangeSink.NOOP, slowGovernor);
    }

    public YierdisFastCommandProcessor(YierdisDbRouter dbRouter, ServerInfoProvider infoProvider, YierdisChangeSink changeSink, SlowCommandGovernor slowGovernor) {
        Objects.requireNonNull(dbRouter, "dbRouter");
        this.dbRouter = dbRouter;
        this.changeSink = changeSink == null ? YierdisChangeSink.NOOP : changeSink;
        CommandSupport support = new CommandSupport(dbRouter, infoProvider, slowGovernor);
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

    public void execute(Command cmd, CommandContext ctx) {
        Objects.requireNonNull(ctx, "ctx");
        ReplyWriter out = ctx.out();
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

        TransactionState tx = null;
        ServerSession s = ctx.serverSessionOrNull();
        if (s != null) {
            tx = s.transaction();
        }
        if (tx != null && tx.active()) {
            boolean isMulti = CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "MULTI");
            boolean isExec = CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "EXEC");
            boolean isDiscard = CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "DISCARD");
            if (!isMulti && !isExec && !isDiscard) {
                // HELLO 属于连接级握手/协商类命令。为避免事务语义被“连接态变更”干扰，保持与 Redis 类似的限制：
                // MULTI 队列中不允许出现 HELLO。
                if (CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "HELLO")) {
                    tx.markAborted();
                    out.error("ERR HELLO is not allowed in MULTI");
                    return;
                }
                String enqueueErr = tx.tryEnqueue(copyArgv(cmd));
                if (enqueueErr != null) {
                    out.error(enqueueErr);
                    return;
                }
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
            handler.execute(cmd, ctx);

            // 变更事件：仅在命令执行成功后触发，并尽量避免对读命令做额外分配。
            if (changeSink != YierdisChangeSink.NOOP && isWriteCommand(cmd)) {
                int dbIndex = 0;
                DbIndexProvider provider = ctx.dbIndexProviderOrNull();
                if (provider != null) {
                    dbIndex = Math.max(0, provider.dbIndex());
                }
                try {
                    changeSink.onChange(new YierdisChangeEvent(dbIndex, copyArgv(cmd)));
                } catch (Throwable ignored) {
                    // best-effort: 事件消费失败不应影响主命令执行路径
                }
            }
        } catch (WrongTypeException e) {
            out.error(e.getMessage());
        } catch (YierdisCommandException e) {
            out.error(e.getMessage());
        } catch (YierdisOffHeapOutOfMemoryException e) {
            out.error("OOM off-heap memory limit exceeded");
        } catch (IllegalArgumentException e) {
            out.error("ERR " + e.getMessage());
        } finally {
            // Ensure a command never leaks a pending maxmemory reservation to the next command.
            // Command implementations are expected to finish (commit/rollback) their own reservations,
            // but this is a defensive last line to keep invariants stable.
            try {
                DbEngine engine = dbRouter.dbFor(ctx.dbIndexProviderOrNull());
                if (engine != null) {
                    engine.eviction().rollbackWriteReservationIfAny();
                }
            } catch (Throwable ignored) {
                // best-effort
            }
        }
    }

    private static boolean isWriteCommand(Command cmd) {
        // 约定：事件流用于 AOF/replication 等外部能力，因此以“可能改变状态”的命令集合为准。
        // 这里不在核心层做“是否真实发生变更”的判定（例如 DEL 0 / SET NX 未写入等）。
        return CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "SET")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "APPEND")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "SETBIT")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "INCR")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "DECR")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "PFADD")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "LPUSH")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "RPUSH")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "LPOP")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "RPOP")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "HSET")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "HDEL")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "SADD")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "SREM")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "ZADD")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "ZREM")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "ZREMRANGEBYSCORE")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "ZREMRANGEBYRANK")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "DEL")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "EXPIRE")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "PEXPIRE")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "EXPIREAT")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "PEXPIREAT")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "PERSIST")
                || CommandSupport.asciiEqualsIgnoreCase(cmd, 0, "FLUSHDB");
    }

    private static byte[][] copyArgv(Command cmd) {
        int argc = cmd == null ? 0 : cmd.argc();
        byte[][] argv = new byte[argc][];
        for (int i = 0; i < argc; i++) {
            argv[i] = cmd.toByteArray(i);
        }
        return argv;
    }

    private static String unknownCommandMessage(Command cmd) {
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

    private static YierdisDbRouter singleDbRouter(DbEngine engine) {
        DbEngine fixed = Objects.requireNonNull(engine, "engine");
        return new YierdisDbRouter() {
            @Override
            public DbEngine dbFor(DbIndexProvider dbIndexProvider) {
                return fixed;
            }

            @Override
            public int databases() {
                return 1;
            }
        };
    }
}
