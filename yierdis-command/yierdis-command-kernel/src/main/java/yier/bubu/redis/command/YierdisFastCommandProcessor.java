package yier.bubu.redis.command;

import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.DbIndexProvider;
import yier.bubu.redis.contract.ExecutionRecord;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.contract.ServerSession;
import yier.bubu.redis.contract.TransactionState;
import yier.bubu.redis.ops.WrongTypeException;
import yier.bubu.redis.ops.YierdisCommandException;
import yier.bubu.redis.runtime.api.YierdisChangeEvent;
import yier.bubu.redis.runtime.api.YierdisChangeSink;
import yier.bubu.redis.runtime.api.YierdisChangeTracking;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A server-side command processor optimized for low allocation.
 * <p>
 * It executes commands and writes replies via {@link ReplyWriter}.
 */
public final class YierdisFastCommandProcessor {
    private static final String NULL_BULK_STRING_ERR = "ERR Protocol error: null bulk string";

    private final CommandRegistry registry;
    private final YierdisChangeSink changeSink;

    public YierdisFastCommandProcessor(CommandModule... modules) {
        this(YierdisChangeSink.NOOP, modules);
    }

    public YierdisFastCommandProcessor(YierdisChangeSink changeSink, Iterable<? extends CommandModule> modules) {
        this(changeSink, toArray(modules));
    }

    private YierdisFastCommandProcessor(YierdisChangeSink changeSink, CommandModule[] modules) {
        this.changeSink = changeSink == null ? YierdisChangeSink.NOOP : changeSink;
        CommandRegistry registry = new CommandRegistry();
        new TransactionCommands(this).register(registry);
        registerExtraModules(registry, modules);
        this.registry = registry;
    }

    public void execute(ExecutionRequest request, CommandContext ctx) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(ctx, "ctx");
        ReplyWriter out = ctx.out();
        int argc = request.argc();
        if (argc <= 0) {
            out.error("ERR empty command");
            return;
        }
        if (request.isNull(0) || request.len(0) == 0) {
            out.error("ERR empty command");
            return;
        }

        // Reject null bulk strings early to avoid NPEs deeper in the DB and data structures.
        // We only allow a null bulk string for PING/ECHO's single message argument (argv[1]).
        boolean allowNullMessage = asciiEqualsIgnoreCase(request, 0, "PING")
                || asciiEqualsIgnoreCase(request, 0, "ECHO");
        for (int i = 1; i < argc; i++) {
            if (!request.isNull(i)) {
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
            boolean isMulti = asciiEqualsIgnoreCase(request, 0, "MULTI");
            boolean isExec = asciiEqualsIgnoreCase(request, 0, "EXEC");
            boolean isDiscard = asciiEqualsIgnoreCase(request, 0, "DISCARD");
            if (!isMulti && !isExec && !isDiscard) {
                CommandSpec<?> spec = registry.spec(request);
                if (spec == null) {
                    tx.markAborted();
                    out.error(unknownCommandMessage(request));
                    return;
                }
                String disallowedInMultiError = spec.disallowedInMultiError();
                if (disallowedInMultiError != null) {
                    tx.markAborted();
                    out.error(disallowedInMultiError);
                    return;
                }
                if (!validateBeforeQueue(spec, request, tx, out)) {
                    return;
                }
                String enqueueErr = tx.tryEnqueue(request);
                if (enqueueErr != null) {
                    out.error(enqueueErr);
                    return;
                }
                out.simpleString("QUEUED");
                return;
            }
        }

        try {
            CommandSpec<?> spec = registry.spec(request);
            if (spec == null) {
                out.error(unknownCommandMessage(request));
                return;
            }
            boolean sinkEnabled = changeSink != YierdisChangeSink.NOOP;
            boolean changed = false;
            if (sinkEnabled) {
                try (YierdisChangeTracking.Scope ignored = YierdisChangeTracking.beginScope()) {
                    executeSpec(spec, request, ctx);
                    changed = YierdisChangeTracking.changedAny();
                }
            } else {
                executeSpec(spec, request, ctx);
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
                    changeSink.onChange(new YierdisChangeEvent(new ExecutionRecord(dbIndex, request)));
                } catch (Throwable ignored) {
                    // best-effort: 事件消费失败不应影响主命令执行路径
                }
            }
        } catch (WrongTypeException e) {
            out.error(e.getMessage());
        } catch (YierdisCommandException e) {
            out.error(e.getMessage());
        } catch (IllegalArgumentException e) {
            out.error("ERR " + e.getMessage());
        }
    }

    private void executeSpec(CommandSpec<?> spec, ExecutionRequest request, CommandContext ctx) {
        CommandParseResult<?> parsed = spec.parse(request);
        if (!parsed.ok()) {
            ctx.out().error(parsed.error().toReplyMessage());
            return;
        }
        executeParsedSpec(spec, parsed.value(), ctx);
    }

    private void executeParsedSpec(CommandSpec<?> spec, Object parsed, CommandContext ctx) {
        spec.executeParsed(parsed, ctx);
    }

    private boolean validateBeforeQueue(CommandSpec<?> spec, ExecutionRequest request, TransactionState tx, ReplyWriter out) {
        CommandParseResult<?> parsed = spec.parse(request);
        if (parsed.ok()) {
            return true;
        }
        tx.markAborted();
        out.error(parsed.error().toReplyMessage());
        return false;
    }

    private static String unknownCommandMessage(ExecutionRequest request) {
        if (request == null || request.argc() <= 0 || request.isNull(0) || request.len(0) <= 0) {
            return "ERR unknown command";
        }
        int len = request.len(0);
        int printable = 0;
        for (int i = 0; i < len; i++) {
            int b = request.byteAt(0, i) & 0xFF;
            if (b >= 0x20 && b <= 0x7E && b != '\'' && b != '\\') {
                printable++;
            }
        }
        if (printable == len && len <= 64) {
            byte[] name = request.readOnlyByteArray(0);
            String s = name == null ? "" : new String(name, java.nio.charset.StandardCharsets.US_ASCII);
            return "ERR unknown command '" + s + "'";
        }
        return "ERR unknown command";
    }

    private static void registerExtraModules(CommandRegistry registry, CommandModule... extraModules) {
        if (extraModules == null || extraModules.length == 0) {
            return;
        }
        for (CommandModule extraModule : extraModules) {
            if (extraModule == null) {
                throw new IllegalArgumentException("extraModules must not contain null");
            }
            extraModule.register(registry);
        }
    }

    private static boolean asciiEqualsIgnoreCase(ExecutionRequest request, int argIndex, String literal) {
        if (literal == null) {
            return false;
        }
        if (request.isNull(argIndex)) {
            return false;
        }
        int len = request.len(argIndex);
        if (len != literal.length()) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            int b = request.byteAt(argIndex, i) & 0xFF;
            int c = literal.charAt(i);
            if (b >= 'A' && b <= 'Z') {
                b |= 0x20;
            }
            if (c >= 'A' && c <= 'Z') {
                c |= 0x20;
            }
            if (b != c) {
                return false;
            }
        }
        return true;
    }

    private static CommandModule[] toArray(Iterable<? extends CommandModule> modules) {
        if (modules == null) {
            return new CommandModule[0];
        }
        List<CommandModule> collected = new ArrayList<>();
        for (CommandModule module : modules) {
            collected.add(module);
        }
        return collected.toArray(new CommandModule[0]);
    }

}
