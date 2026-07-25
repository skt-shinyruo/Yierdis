package yier.bubu.redis.command.kernel;

import yier.bubu.redis.command.api.CommandParseResult;
import yier.bubu.redis.command.api.CommandDefinition;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.TransactionState;

import java.util.Objects;

final class TransactionQueuePolicy {
    void markActiveTransactionAborted(CommandPreparationContext ctx) {
        Objects.requireNonNull(ctx, "ctx");

        TransactionState tx = ctx.session().transaction();
        if (tx.active()) {
            tx.markAborted();
        }
    }

    PreparedCommand queueIfNeeded(
            ExecutionRequest request,
            CommandPreparationContext ctx,
            CommandRegistry registry
    ) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(ctx, "ctx");
        Objects.requireNonNull(registry, "registry");

        TransactionState tx = ctx.session().transaction();
        if (!tx.active()) {
            return null;
        }

        CommandDefinition<?> definition = registry.definition(request);
        if (definition == null) {
            return PreparedCommands.error(
                    CommandRequestSupport.unknownCommandMessage(request),
                    tx::markAborted
            );
        }

        if (definition.syntax().transactionPolicy() == TransactionPolicy.TRANSACTION_CONTROL) {
            return null;
        }
        if (definition.syntax().transactionPolicy() == TransactionPolicy.DISALLOWED_IN_MULTI) {
            return PreparedCommands.error(
                    "ERR " + definition.syntax().nameUpper() + " is not allowed in MULTI",
                    tx::markAborted
            );
        }

        CommandParseResult<?> parsed = definition.parse(request);
        if (!parsed.ok()) {
            return PreparedCommands.error(parsed.error().toReplyMessage(), tx::markAborted);
        }

        return PreparedCommands.fixed(ReplyShapes.maximum(), execution -> {
            String enqueueError = tx.tryEnqueue(request);
            if (enqueueError == null) {
                execution.reply().simpleString("QUEUED");
            } else {
                execution.reply().error(enqueueError);
            }
        });
    }
}
