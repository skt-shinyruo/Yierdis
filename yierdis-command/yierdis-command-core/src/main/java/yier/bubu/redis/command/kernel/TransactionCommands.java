package yier.bubu.redis.command.kernel;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandInvocation;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.execution.api.ValidationResult;

/** Transaction control commands and the prepared EXEC reply envelope. */
final class TransactionCommands implements CommandModule {
    private static final String EXEC_ABORT = "EXECABORT Transaction discarded because of previous errors.";

    private final CommandDispatcher dispatcher;

    TransactionCommands(CommandDispatcher dispatcher) {
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandSpec(syntax("MULTI"), this::multi));
        registration.register(new CommandSpec(syntax("DISCARD"), this::discard));
        registration.register(new CommandSpec(syntax("EXEC"), this::exec));
    }

    private static CommandSyntax syntax(String name) {
        return new CommandSyntax(
                name,
                CommandArity.exact(1),
                CommandKeySpec.NONE,
                TransactionPolicy.TRANSACTION_CONTROL
        );
    }

    private CommandInvocation multi(CommandArgs args) {
        return session -> {
            TransactionState tx = session.transaction();
            if (tx.active()) {
                return error("ERR MULTI calls can not be nested");
            }
            return yier.bubu.redis.execution.api.PreparedCommands.action(
                    ReplyShapes.simpleString("OK"),
                    context -> {
                        tx.begin();
                        return CommandResult.reply(RedisReplies.simpleString("OK"));
                    }
            );
        };
    }

    private CommandInvocation discard(CommandArgs args) {
        return session -> {
            TransactionState tx = session.transaction();
            if (!tx.active()) {
                return error("ERR DISCARD without MULTI");
            }
            return yier.bubu.redis.execution.api.PreparedCommands.action(
                    ReplyShapes.simpleString("OK"),
                    context -> {
                        tx.discard();
                        return CommandResult.reply(RedisReplies.simpleString("OK"));
                    }
            );
        };
    }

    private CommandInvocation exec(CommandArgs args) {
        return session -> prepareExec(session);
    }

    private PreparedCommand prepareExec(CommandSession session) {
        TransactionState tx = session.transaction();
        if (!tx.active()) {
            return error("ERR EXEC without MULTI");
        }
        if (tx.aborted()) {
            return yier.bubu.redis.execution.api.PreparedCommands.action(
                    ReplyShapes.error(EXEC_ABORT),
                    context -> {
                        tx.discard();
                        return CommandResult.error(EXEC_ABORT);
                    }
            );
        }
        if (tx.size() <= 1) {
            return preparedExactExec(tx, session);
        }
        return preparedDynamicExec(tx, session);
    }

    private PreparedCommand preparedExactExec(TransactionState tx, CommandSession session) {
        ArrayList<PreparedCommand> children = new ArrayList<>(tx.size());
        ArrayList<ReplyShape> shapes = new ArrayList<>(tx.size());
        try {
            tx.forEachQueued(request -> {
                PreparedCommand child = dispatcher.prepareReplay(session, request);
                children.add(child);
                shapes.add(child.replyShape());
            });
        } catch (RuntimeException | Error failure) {
            closeChildrenReverse(children, failure);
            throw failure;
        }
        return new PreparedExec(tx, dispatcher, session, children, ReplyShapes.array(shapes), false);
    }

    private PreparedCommand preparedDynamicExec(TransactionState tx, CommandSession session) {
        return new PreparedExec(tx, dispatcher, session, new ArrayList<>(), ReplyShapes.maximum(), true);
    }

    private static PreparedCommand error(String message) {
        return yier.bubu.redis.execution.api.PreparedCommands.ready(RedisReplies.error(message));
    }

    private static void closeChildrenReverse(List<PreparedCommand> children, Throwable primary) {
        Throwable failure = primary;
        for (int index = children.size() - 1; index >= 0; index--) {
            PreparedCommand child = children.get(index);
            if (child != null) {
                try {
                    child.close();
                } catch (RuntimeException | Error closeFailure) {
                    if (failure == null) {
                        failure = closeFailure;
                    } else if (failure != closeFailure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
                children.set(index, null);
            }
        }
        if (primary == null && failure != null) {
            rethrow(failure);
        }
    }

    private static void closeSuppressing(Throwable primary, Runnable close) {
        try {
            close.run();
        } catch (RuntimeException | Error closeFailure) {
            if (primary == null) {
                throw closeFailure;
            }
            if (primary != closeFailure) {
                primary.addSuppressed(closeFailure);
            }
        }
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        throw (Error) failure;
    }

    private static final class PreparedExec implements PreparedCommand {
        private final TransactionState tx;
        private final CommandDispatcher dispatcher;
        private final CommandSession session;
        private final ArrayList<PreparedCommand> children;
        private final ReplyShape replyShape;
        private final boolean prepareDuringExecution;
        private boolean closed;

        private PreparedExec(
                TransactionState tx,
                CommandDispatcher dispatcher,
                CommandSession session,
                ArrayList<PreparedCommand> children,
                ReplyShape replyShape,
                boolean prepareDuringExecution
        ) {
            this.tx = Objects.requireNonNull(tx, "tx");
            this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
            this.session = Objects.requireNonNull(session, "session");
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
            Throwable primary = null;
            int next = 0;
            try {
                if (!prepareDuringExecution && queued.size() != children.size()) {
                    throw new IllegalStateException("transaction queue changed after EXEC preparation");
                }
                context.reply().arrayHeader(queued.size());
                for (int index = 0; index < queued.size(); index++) {
                    ExecutionRequest request = queued.get(index);
                    PreparedCommand child = null;
                    try {
                        child = prepareDuringExecution
                                ? prepareCurrentChild(request)
                                : children.get(index);
                        executeChild(context, request, child);
                    } catch (RuntimeException | Error failure) {
                        primary = failure;
                        closeChild(index, child, primary);
                        closeRequest(request, primary);
                        next = index + 1;
                        throw failure;
                    }
                    closeChild(index, child, null);
                    next = index + 1;
                    closeRequest(request, null);
                }
            } catch (RuntimeException | Error failure) {
                primary = failure;
                throw failure;
            } finally {
                closeQueuedTail(queued, next, primary);
                closeChildrenFrom(next, primary);
            }
        }

        private PreparedCommand prepareCurrentChild(ExecutionRequest request) {
            for (;;) {
                PreparedCommand child = dispatcher.prepareReplay(session, request);
                ValidationResult validation;
                try {
                    validation = child.validateBeforeExecute();
                } catch (RuntimeException | Error failure) {
                    closeSuppressing(failure, child::close);
                    throw failure;
                }
                if (validation != ValidationResult.STALE) {
                    return child;
                }
                closeSuppressing(null, child::close);
            }
        }

        private static void executeChild(
                CommandExecutionContext context,
                ExecutionRequest request,
                PreparedCommand child
        ) {
            try (CommandExecutionContext childContext = CommandExecutionContext.forRequest(
                    context.session(), context.reply(), request
            )) {
                child.execute(childContext);
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            closeChildrenReverse(children, null);
        }

        private void closeChild(int index, PreparedCommand child, Throwable primary) {
            if (!prepareDuringExecution && index < children.size() && children.get(index) == child) {
                children.set(index, null);
            }
            if (child != null) {
                closeSuppressing(primary, child::close);
            }
        }

        private static void closeRequest(ExecutionRequest request, Throwable primary) {
            closeSuppressing(primary, request::close);
        }

        private void closeChildrenFrom(int start, Throwable primary) {
            for (int index = Math.max(0, start); index < children.size(); index++) {
                PreparedCommand child = children.get(index);
                if (child != null) {
                    children.set(index, null);
                    closeSuppressing(primary, child::close);
                }
            }
        }

        private static void closeQueuedTail(List<ExecutionRequest> requests, int from, Throwable primary) {
            for (int index = Math.max(0, from); index < requests.size(); index++) {
                closeRequest(requests.get(index), primary);
            }
        }
    }
}
