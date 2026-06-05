package yier.bubu.redis.command.kernel;

import yier.bubu.redis.command.api.CommandParseResult;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.TransactionState;

import java.util.Objects;

final class TransactionQueuePolicy {
    boolean queueIfNeeded(ExecutionRequest request, CommandContext ctx, CommandRegistry registry) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(ctx, "ctx");
        Objects.requireNonNull(registry, "registry");

        TransactionState tx = ctx.transactionSession().transaction();
        if (!tx.active() || isTransactionControl(request)) {
            return false;
        }

        RedisReplyWriter out = ctx.out();
        CommandSpec<?> spec = registry.spec(request);
        if (spec == null) {
            tx.markAborted();
            out.error(CommandRequestSupport.unknownCommandMessage(request));
            return true;
        }

        String disallowedInMultiError = spec.disallowedInMultiError();
        if (disallowedInMultiError != null) {
            tx.markAborted();
            out.error(disallowedInMultiError);
            return true;
        }

        if (!validateBeforeQueue(spec, request, tx, out)) {
            return true;
        }

        String enqueueErr = tx.tryEnqueue(request);
        if (enqueueErr != null) {
            out.error(enqueueErr);
            return true;
        }
        out.simpleString("QUEUED");
        return true;
    }

    private boolean validateBeforeQueue(
            CommandSpec<?> spec,
            ExecutionRequest request,
            TransactionState tx,
            RedisReplyWriter out
    ) {
        CommandParseResult<?> parsed = spec.parse(request);
        if (parsed.ok()) {
            return true;
        }
        tx.markAborted();
        out.error(parsed.error().toReplyMessage());
        return false;
    }

    private static boolean isTransactionControl(ExecutionRequest request) {
        return CommandRequestSupport.asciiEqualsIgnoreCase(request, 0, "MULTI")
                || CommandRequestSupport.asciiEqualsIgnoreCase(request, 0, "EXEC")
                || CommandRequestSupport.asciiEqualsIgnoreCase(request, 0, "DISCARD");
    }
}
