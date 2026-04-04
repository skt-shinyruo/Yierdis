package yier.bubu.redis.command;

import yier.bubu.redis.offheap.api.OffHeapOutOfMemoryException;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.ops.WrongTypeException;
import yier.bubu.redis.ops.YierdisCommandException;
import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.Command;
import yier.bubu.redis.contract.DbIndexProvider;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.contract.ServerSession;
import yier.bubu.redis.contract.TransactionState;
import yier.bubu.redis.runtime.api.YierdisChangeEvent;
import yier.bubu.redis.runtime.api.YierdisChangeTracking;
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
        this(singleDbRouter(engine), infoProvider, changeSink, slowGovernor, new CommandModule[0]);
    }

    public YierdisFastCommandProcessor(DbEngine engine, ServerInfoProvider infoProvider, SlowCommandGovernor slowGovernor, CommandModule... extraModules) {
        this(singleDbRouter(engine), infoProvider, YierdisChangeSink.NOOP, slowGovernor, extraModules);
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
        this(dbRouter, infoProvider, changeSink, slowGovernor, new CommandModule[0]);
    }

    public YierdisFastCommandProcessor(YierdisDbRouter dbRouter, ServerInfoProvider infoProvider, SlowCommandGovernor slowGovernor, CommandModule... extraModules) {
        this(dbRouter, infoProvider, YierdisChangeSink.NOOP, slowGovernor, extraModules);
    }

    public YierdisFastCommandProcessor(YierdisDbRouter dbRouter, ServerInfoProvider infoProvider, YierdisChangeSink changeSink, SlowCommandGovernor slowGovernor, CommandModule... extraModules) {
        Objects.requireNonNull(dbRouter, "dbRouter");
        this.dbRouter = dbRouter;
        this.changeSink = changeSink == null ? YierdisChangeSink.NOOP : changeSink;
        CommandSupport support = new CommandSupport(dbRouter, infoProvider, slowGovernor);
        CommandRegistry registry = new CommandRegistry();
        new TransactionCommands(support, this).register(registry);
        new CoreConnectionCommands(support).register(registry);
        new KeyCommands(support).register(registry);
        new StringCommands(support).register(registry);
        new HllCommands(support).register(registry);
        new ListCommands(support).register(registry);
        new HashCommands(support).register(registry);
        new SetCommands(support).register(registry);
        new ZSetCommands(support).register(registry);
        registerExtraModules(registry, extraModules);
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
                CommandSpec spec = registry.spec(cmd);
                String disallowedInMultiError = spec == null ? null : spec.disallowedInMultiError();
                if (disallowedInMultiError != null) {
                    tx.markAborted();
                    out.error(disallowedInMultiError);
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
            CommandSpec spec = registry.spec(cmd);
            if (spec == null) {
                out.error(unknownCommandMessage(cmd));
                return;
            }
            CommandModule.Handler handler = spec.handler();
            boolean sinkEnabled = changeSink != YierdisChangeSink.NOOP;
            boolean changed = false;
            if (sinkEnabled) {
                try (YierdisChangeTracking.Scope ignored = YierdisChangeTracking.beginScope()) {
                    handler.execute(cmd, ctx);
                    changed = YierdisChangeTracking.changedAny();
                }
            } else {
                handler.execute(cmd, ctx);
            }

            // 变更事件：仅在命令执行成功后触发；仅当本次命令产生“真实变更”（Keyspace/Value/TTL 元数据）时 emit。
            // 该判定由 DB/ops 层在真实写入点打点，命令层仅按事实 gate emit，避免“写命令名单”漂移。
            if (sinkEnabled && changed) {
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
        } catch (OffHeapOutOfMemoryException e) {
            out.error("OOM off-heap memory limit exceeded");
        } catch (IllegalArgumentException e) {
            out.error("ERR " + e.getMessage());
        }
    }

    private static byte[][] copyArgv(Command cmd) {
        int argc = cmd == null ? 0 : cmd.argc();
        byte[][] argv = new byte[argc][];
        for (int i = 0; i < argc; i++) {
            if (cmd.isNull(i)) {
                argv[i] = null;
                continue;
            }
            int len = cmd.len(i);
            if (len < 0) {
                // Defensive: treat negative length as a null bulk string.
                argv[i] = null;
                continue;
            }
            if (len == 0) {
                argv[i] = new byte[0];
                continue;
            }
            byte[] out = new byte[len];
            cmd.copyToByteArray(i, out, 0);
            argv[i] = out;
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

    private static void registerExtraModules(CommandRegistry registry, CommandModule... extraModules) {
        if (extraModules == null || extraModules.length == 0) {
            return;
        }
        CommandModule.Registration registrar = new CommandModule.Registration() {
            @Override
            public void register(String name, CommandSpec spec) {
                registry.register(name, spec);
            }

            @Override
            public int commandCount() {
                return registry.commandCount();
            }

            @Override
            public boolean containsUpperName(String nameUpper) {
                return registry.containsUpperName(nameUpper);
            }

            @Override
            public String[] upperNamesSorted() {
                return registry.upperNamesSorted();
            }
        };
        for (CommandModule extraModule : extraModules) {
            if (extraModule == null) {
                throw new IllegalArgumentException("extraModules must not contain null");
            }
            extraModule.register(registrar);
        }
    }
}
