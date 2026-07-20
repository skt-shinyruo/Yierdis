package yier.bubu.redis.command.kernel;

import yier.bubu.redis.command.api.CommandParseResult;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.TransactionState;

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
    private final CommandExceptionTranslator exceptionTranslator = new CommandExceptionTranslator();

    public YierdisFastCommandProcessor(CommandRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    public void execute(ExecutionRequest request, CommandContext ctx) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(ctx, "ctx");
        try {
            executeCommand(request, ctx);
        } finally {
            // CommandSupport 复用的 DB view 可能持有同一 context；统一释放借用，避免其继续强引用已完成命令的 argv。
            ctx.releaseMutationContext();
        }
    }

    private void executeCommand(ExecutionRequest request, CommandContext ctx) {
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

        // RESP arrays may legally carry null bulk strings into ExecutionRequest.
        // Command semantics still reject them by default so DB and command implementations
        // never see unexpected nulls. The only allowed form remains PING/ECHO with a
        // single message argument at argv[1].
        boolean allowNullMessage = CommandRequestSupport.asciiEqualsIgnoreCase(request, 0, "PING")
                || CommandRequestSupport.asciiEqualsIgnoreCase(request, 0, "ECHO");
        for (int i = 1; i < argc; i++) {
            if (!request.isNull(i)) {
                continue;
            }
            if (allowNullMessage && argc == 2 && i == 1) {
                continue;
            }
            transactionQueuePolicy.markActiveTransactionAborted(ctx);
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
            executeSpec(spec, request, ctx);
        });
    }

    private void executeSpec(CommandSpec<?> spec, ExecutionRequest request, CommandContext ctx) {
        CommandParseResult<?> parsed = spec.parse(request);
        if (!parsed.ok()) {
            transactionQueuePolicy.markActiveTransactionAborted(ctx);
            ctx.out().error(parsed.error().toReplyMessage());
            return;
        }
        executeParsedSpec(spec, parsed.value(), ctx);
    }

    private void executeParsedSpec(CommandSpec<?> spec, Object parsed, CommandContext ctx) {
        spec.executeParsed(parsed, ctx);
    }

}
