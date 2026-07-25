package yier.bubu.redis.command.kernel;

import yier.bubu.redis.command.api.CommandParseResult;
import yier.bubu.redis.command.api.CommandDefinition;
import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;

import java.util.Objects;

/**
 * A server-side command processor optimized for low allocation.
 * <p>
 * It validates and prepares commands before the executor reserves reply capacity.
 */
public final class YierdisFastCommandProcessor {
    private static final String NULL_BULK_STRING_ERR = "ERR Protocol error: null bulk string";

    private final CommandRegistry registry;
    private final TransactionQueuePolicy transactionQueuePolicy = new TransactionQueuePolicy();
    private final CommandExceptionTranslator exceptionTranslator = new CommandExceptionTranslator();

    public YierdisFastCommandProcessor(CommandRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    public PreparedCommand prepare(ExecutionRequest request, CommandPreparationContext ctx) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(ctx, "ctx");
        return prepareCommand(request, ctx, true);
    }

    PreparedCommand prepareQueued(ExecutionRequest request, CommandPreparationContext ctx) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(ctx, "ctx");
        return prepareCommand(request, ctx, false);
    }

    private PreparedCommand prepareCommand(
            ExecutionRequest request,
            CommandPreparationContext ctx,
            boolean applyTransactionQueuePolicy
    ) {
        int argc = request.argc();
        if (argc <= 0) {
            return PreparedCommands.error("ERR empty command");
        }
        if (request.isNull(0) || request.len(0) == 0) {
            return PreparedCommands.error("ERR empty command");
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
            return PreparedCommands.error(
                    NULL_BULK_STRING_ERR,
                    () -> transactionQueuePolicy.markActiveTransactionAborted(ctx)
            );
        }

        if (applyTransactionQueuePolicy) {
            PreparedCommand queued = transactionQueuePolicy.queueIfNeeded(request, ctx, registry);
            if (queued != null) {
                return queued;
            }
        }

        return exceptionTranslator.prepare(() -> prepareDefinition(request, ctx));
    }

    private PreparedCommand prepareDefinition(ExecutionRequest request, CommandPreparationContext ctx) {
        CommandDefinition<?> definition = registry.definition(request);
        if (definition == null) {
            return PreparedCommands.error(CommandRequestSupport.unknownCommandMessage(request));
        }
        CommandParseResult<?> parsed = definition.parse(request);
        if (!parsed.ok()) {
            return PreparedCommands.error(
                    parsed.error().toReplyMessage(),
                    () -> transactionQueuePolicy.markActiveTransactionAborted(ctx)
            );
        }
        return prepareParsedDefinition(definition, parsed.value(), ctx);
    }

    @SuppressWarnings("unchecked")
    private static <T> PreparedCommand prepareParsedDefinition(
            CommandDefinition<T> definition,
            Object parsed,
            CommandPreparationContext ctx
    ) {
        return definition.preparer().prepare((T) parsed, ctx);
    }
}
