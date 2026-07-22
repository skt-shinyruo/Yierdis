package yier.bubu.redis.command.kernel;

import yier.bubu.redis.command.api.CommandParseResult;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.TransactionState;

import java.util.Objects;

final class TransactionQueuePolicy {
    void markActiveTransactionAborted(CommandContext ctx) {
        Objects.requireNonNull(ctx, "ctx");

        TransactionState tx = ctx.transactionSession().transaction();
        if (tx.active()) {
            tx.markAborted();
        }
    }

    boolean queueIfNeeded(ExecutionRequest request, CommandContext ctx, CommandRegistry registry) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(ctx, "ctx");
        Objects.requireNonNull(registry, "registry");

        TransactionState tx = ctx.transactionSession().transaction();
        if (!tx.active()) {
            return false;
        }

        RedisReplyWriter out = ctx.out();
        CommandSpec<?> spec = registry.spec(request);
        if (spec == null) {
            tx.markAborted();
            out.error(CommandRequestSupport.unknownCommandMessage(request));
            return true;
        }

        if (spec.syntax().transactionPolicy() == TransactionPolicy.TRANSACTION_CONTROL) {
            return false;
        }
        if (spec.syntax().transactionPolicy() == TransactionPolicy.DISALLOWED_IN_MULTI) {
            tx.markAborted();
            out.error("ERR " + spec.syntax().nameUpper() + " is not allowed in MULTI");
            return true;
        }

        CommandParseResult<?> parsed = spec.parse(request);
        if (!parsed.ok()) {
            tx.markAborted();
            out.error(parsed.error().toReplyMessage());
            return true;
        }

        String enqueueError = tx.tryEnqueue(request);
        if (enqueueError != null) {
            out.error(enqueueError);
            return true;
        }
        out.simpleString("QUEUED");
        return true;
    }
}
