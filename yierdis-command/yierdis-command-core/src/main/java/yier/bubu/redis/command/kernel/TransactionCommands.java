package yier.bubu.redis.command.kernel;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDefinition;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.execution.api.ValidationResult;

/** Transaction control commands and the prepared EXEC reply envelope. */
final class TransactionCommands implements CommandModule {
    private final YierdisFastCommandProcessor processor;

    TransactionCommands(YierdisFastCommandProcessor processor) {
        this.processor = Objects.requireNonNull(processor, "processor");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandDefinition<>(
                syntax("MULTI"), CommandParsers.request(), this::multi));
        registration.register(new CommandDefinition<>(
                syntax("DISCARD"), CommandParsers.request(), this::discard));
        registration.register(new CommandDefinition<>(
                syntax("EXEC"), CommandParsers.request(), this::exec));
    }

    private static CommandSyntax syntax(String name) {
        return new CommandSyntax(
                name,
                CommandArity.exact(1),
                CommandKeySpec.NONE,
                TransactionPolicy.TRANSACTION_CONTROL
        );
    }

    private PreparedCommand multi(ExecutionRequest request, CommandPreparationContext context) {
        TransactionState tx = context.session().transaction();
        if (tx.active()) {
            return PreparedCommands.error("ERR MULTI calls can not be nested");
        }
        return PreparedCommands.fixed(ReplyShapes.simpleString("OK"), execution -> {
            tx.begin();
            execution.reply().simpleString("OK");
        });
    }

    private PreparedCommand discard(ExecutionRequest request, CommandPreparationContext context) {
        TransactionState tx = context.session().transaction();
        if (!tx.active()) {
            return PreparedCommands.error("ERR DISCARD without MULTI");
        }
        return PreparedCommands.fixed(ReplyShapes.simpleString("OK"), execution -> {
            tx.discard();
            execution.reply().simpleString("OK");
        });
    }

    private PreparedCommand exec(ExecutionRequest request, CommandPreparationContext context) {
        TransactionState tx = context.session().transaction();
        if (!tx.active()) {
            return PreparedCommands.error("ERR EXEC without MULTI");
        }
        if (tx.aborted()) {
            return PreparedCommands.fixed(
                    ReplyShapes.error("EXECABORT Transaction discarded because of previous errors."),
                    execution -> {
                        tx.discard();
                        execution.reply().error("EXECABORT Transaction discarded because of previous errors.");
                }
            );
        }

        if (tx.size() > 1) {
            return new PreparedExec(tx, processor, new ArrayList<>(), ReplyShapes.maximum(), true);
        }

        ArrayList<PreparedCommand> children = new ArrayList<>(tx.size());
        ArrayList<ReplyShape> shapes = new ArrayList<>(tx.size());
        try {
            tx.forEachQueued(queued -> {
                PreparedCommand child = processor.prepareQueued(queued, context);
                children.add(child);
                shapes.add(child.replyShape());
            });
        } catch (RuntimeException | Error failure) {
            closeChildrenReverse(children);
            throw failure;
        }
        return new PreparedExec(tx, processor, children, ReplyShapes.array(shapes), false);
    }

    private static void closeChildrenReverse(List<PreparedCommand> children) {
        RuntimeException failure = null;
        for (int index = children.size() - 1; index >= 0; index--) {
            PreparedCommand child = children.get(index);
            if (child == null) {
                continue;
            }
            try {
                child.close();
            } catch (RuntimeException closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static final class PreparedExec implements PreparedCommand {
        private final TransactionState tx;
        private final YierdisFastCommandProcessor processor;
        private final ArrayList<PreparedCommand> children;
        private final ReplyShape replyShape;
        private final boolean prepareDuringExecution;
        private boolean closed;

        private PreparedExec(
                TransactionState tx,
                YierdisFastCommandProcessor processor,
                ArrayList<PreparedCommand> children,
                ReplyShape replyShape,
                boolean prepareDuringExecution
        ) {
            this.tx = Objects.requireNonNull(tx, "tx");
            this.processor = Objects.requireNonNull(processor, "processor");
            this.children = Objects.requireNonNull(children, "children");
            this.replyShape = Objects.requireNonNull(replyShape, "replyShape");
            this.prepareDuringExecution = prepareDuringExecution;
        }

        @Override
        public ReplyShape replyShape() {
            return replyShape;
        }

        @Override
        public ValidationResult validateBeforeExecute() {
            if (prepareDuringExecution) {
                return ValidationResult.VALID;
            }
            for (PreparedCommand child : children) {
                if (child != null && child.validateBeforeExecute() == ValidationResult.STALE) {
                    return ValidationResult.STALE;
                }
            }
            return ValidationResult.VALID;
        }

        @Override
        public void execute(CommandExecutionContext context) {
            List<ExecutionRequest> queued = tx.drain();
            if (!prepareDuringExecution && queued.size() != children.size()) {
                closeQueuedRequests(queued, 0);
                throw new IllegalStateException("transaction queue changed after EXEC preparation");
            }

            int firstUnclosed = 0;
            try {
                context.reply().arrayHeader(queued.size());
                for (int index = 0; index < queued.size(); index++) {
                    ExecutionRequest request = queued.get(index);
                    firstUnclosed = index + 1;
                    if (prepareDuringExecution) {
                        try (request) {
                            executeCurrentChild(context, request);
                        }
                    } else {
                        try (request;
                             CommandExecutionContext childContext = CommandExecutionContext.forRequest(
                                     context.session(), context.reply(), request)) {
                            children.get(index).execute(childContext);
                        } finally {
                            closeChild(index);
                        }
                    }
                }
            } finally {
                closeQueuedRequests(queued, firstUnclosed);
                closeChildrenFrom(firstUnclosed);
            }
        }

        private void executeCurrentChild(CommandExecutionContext context, ExecutionRequest request) {
            for (;;) {
                try (PreparedCommand child = processor.prepareQueued(
                        request,
                        new CommandPreparationContext(context.session())
                )) {
                    if (child.validateBeforeExecute() == ValidationResult.STALE) {
                        continue;
                    }
                    try (CommandExecutionContext childContext = CommandExecutionContext.forRequest(
                            context.session(), context.reply(), request
                    )) {
                        child.execute(childContext);
                    }
                    return;
                }
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            closeChildrenReverse(children);
            for (int index = 0; index < children.size(); index++) {
                children.set(index, null);
            }
        }

        private void closeChild(int index) {
            PreparedCommand child = children.get(index);
            if (child == null) {
                return;
            }
            children.set(index, null);
            child.close();
        }

        private void closeChildrenFrom(int start) {
            for (int index = Math.max(0, start); index < children.size(); index++) {
                closeChild(index);
            }
        }

        private static void closeQueuedRequests(List<ExecutionRequest> requests, int from) {
            for (int index = Math.max(0, from); index < requests.size(); index++) {
                try {
                    requests.get(index).close();
                } catch (RuntimeException | Error ignored) {
                    // Preserve the original command failure while releasing transaction-owned tail requests.
                }
            }
        }
    }
}
