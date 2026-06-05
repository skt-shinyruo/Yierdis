package yier.bubu.redis.command.kernel;

import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParseResult;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A server-side command processor optimized for low allocation.
 * <p>
 * It executes commands and writes replies via {@link RedisReplyWriter}.
 */
public final class YierdisFastCommandProcessor {
    private static final String NULL_BULK_STRING_ERR = "ERR Protocol error: null bulk string";

    private final CommandRegistry registry;
    private final TransactionQueuePolicy transactionQueuePolicy = new TransactionQueuePolicy();
    private final CommandChangeEmitter changeEmitter;
    private final CommandExceptionTranslator exceptionTranslator = new CommandExceptionTranslator();

    public YierdisFastCommandProcessor(CommandModule... modules) {
        this(CommandChangeEmitter.noop(), modules);
    }

    public YierdisFastCommandProcessor(YierdisCommandProcessorOptions options, CommandModule... modules) {
        this(CommandChangeEmitter.fromOptions(options), modules);
    }

    public YierdisFastCommandProcessor(
            YierdisCommandProcessorOptions options,
            Iterable<? extends CommandModule> modules
    ) {
        this(CommandChangeEmitter.fromOptions(options), toArray(modules));
    }

    private YierdisFastCommandProcessor(CommandChangeEmitter changeEmitter, CommandModule[] modules) {
        this.changeEmitter = Objects.requireNonNull(changeEmitter, "changeEmitter");
        CommandRegistry registry = new CommandRegistry();
        new TransactionCommands(this).register(registry);
        registerExtraModules(registry, modules);
        this.registry = registry;
    }

    public void execute(ExecutionRequest request, CommandContext ctx) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(ctx, "ctx");
        RedisReplyWriter out = ctx.out();
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
        boolean allowNullMessage = CommandRequestSupport.asciiEqualsIgnoreCase(request, 0, "PING")
                || CommandRequestSupport.asciiEqualsIgnoreCase(request, 0, "ECHO");
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

        if (transactionQueuePolicy.queueIfNeeded(request, ctx, registry)) {
            return;
        }

        exceptionTranslator.run(out, () -> {
            CommandSpec<?> spec = registry.spec(request);
            if (spec == null) {
                out.error(CommandRequestSupport.unknownCommandMessage(request));
                return;
            }
            changeEmitter.execute(request, ctx, () -> executeSpec(spec, request, ctx));
        });
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
