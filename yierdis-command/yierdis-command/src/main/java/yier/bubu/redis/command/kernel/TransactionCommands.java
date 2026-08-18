package yier.bubu.redis.command.kernel;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandArity;
import java.util.function.Function;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.common.command.ResultUnknownException;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.execution.api.ValidationResult;

/** 事务控制命令以及 EXEC 的延迟回复所有权边界。 */
final class TransactionCommands {
    private static final String EXEC_ABORT = "EXECABORT Transaction discarded because of previous errors.";

    private final CommandDispatcher dispatcher;

    TransactionCommands(CommandDispatcher dispatcher) {
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    }

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

    private Function<CommandSession, PreparedCommand> multi(CommandArgs args) {
        return session -> {
            TransactionState tx = session.transaction();
            if (tx.active()) {
                return error("ERR MULTI calls can not be nested");
            }
            return PreparedCommands.action(
                    ReplyShapes.simpleString("OK"),
                    context -> {
                        tx.begin();
                        return CommandResult.reply(RedisReplies.simpleString("OK"));
                    }
            );
        };
    }

    private Function<CommandSession, PreparedCommand> discard(CommandArgs args) {
        return session -> {
            TransactionState tx = session.transaction();
            if (!tx.active()) {
                return error("ERR DISCARD without MULTI");
            }
            return PreparedCommands.action(
                    ReplyShapes.simpleString("OK"),
                    context -> {
                        tx.discard();
                        return CommandResult.reply(RedisReplies.simpleString("OK"));
                    }
            );
        };
    }

    private Function<CommandSession, PreparedCommand> exec(CommandArgs args) {
        return session -> prepareExec(session);
    }

    private PreparedCommand prepareExec(CommandSession session) {
        TransactionState tx = session.transaction();
        if (!tx.active()) {
            return error("ERR EXEC without MULTI");
        }
        if (tx.aborted()) {
            return PreparedCommands.action(
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
        try {
            tx.forEachQueued(request -> {
                PreparedCommand child = dispatcher.prepareReplay(session, request);
                addOwnedChild(children, child);
            });
            ReplyShape reservationShape = children.isEmpty()
                    ? ReplyShapes.array(List.of())
                    : ReplyShapes.maximum();
            return new PreparedExec(tx, dispatcher, session, children, reservationShape, false);
        } catch (RuntimeException | Error failure) {
            closeChildrenReverse(children, failure);
            throw failure;
        }
    }

    private PreparedCommand preparedDynamicExec(TransactionState tx, CommandSession session) {
        return new PreparedExec(tx, dispatcher, session, new ArrayList<>(), ReplyShapes.maximum(), true);
    }

    private static PreparedCommand error(String message) {
        return PreparedCommands.ready(RedisReplies.error(message));
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

    private static void addOwnedChild(
            ArrayList<PreparedCommand> children,
            PreparedCommand child
    ) {
        try {
            children.add(child);
        } catch (RuntimeException | Error failure) {
            // prepareReplay 已转移 child 所有权；发布到清理列表失败时必须在当前栈帧归还。
            closeSuppressing(failure, child::close);
            throw failure;
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
        private final ReplyShape reservationShape;
        private final boolean prepareDuringExecution;
        private List<ExecutionRequest> drainedRequests = List.of();
        private boolean executed;
        private boolean closed;

        private PreparedExec(
                TransactionState tx,
                CommandDispatcher dispatcher,
                CommandSession session,
                ArrayList<PreparedCommand> children,
                ReplyShape reservationShape,
                boolean prepareDuringExecution
        ) {
            this.tx = Objects.requireNonNull(tx, "tx");
            this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
            this.session = Objects.requireNonNull(session, "session");
            this.children = Objects.requireNonNull(children, "children");
            this.reservationShape = Objects.requireNonNull(reservationShape, "reservationShape");
            this.prepareDuringExecution = prepareDuringExecution;
        }

        @Override
        public ReplyShape reservationShape() {
            return reservationShape;
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
        public CommandResult execute(CommandSession context) {
            Objects.requireNonNull(context, "context");
            if (closed || executed) {
                throw new IllegalStateException("prepared EXEC is no longer executable");
            }
            executed = true;
            boolean childExecutionStarted = false;
            try {
                List<ExecutionRequest> queued = Objects.requireNonNull(
                        tx.drain(), "transaction drain returned null");
                drainedRequests = queued;
                if (!prepareDuringExecution && queued.size() != children.size()) {
                    throw new IllegalStateException("transaction queue changed after EXEC preparation");
                }
                ArrayList<RedisReply> replies = new ArrayList<>(queued.size());
                boolean closeAfterReply = false;
                for (int index = 0; index < queued.size(); index++) {
                    ExecutionRequest request = queued.get(index);
                    PreparedCommand child = prepareDuringExecution
                            ? prepareCurrentChild(request)
                            : children.get(index);
                    if (prepareDuringExecution) {
                        addOwnedChild(children, child);
                    }
                    childExecutionStarted = true;
                    CommandResult result = executeChild(child);
                    RedisReply reply = result.reply();
                    if (reply instanceof RedisReply.ControlError controlError) {
                        reply = RedisReplies.error(controlError.message());
                    }
                    replies.add(reply);
                    closeAfterReply |= result.closeAfterReply();
                }
                RedisReply aggregate = RedisReplies.array(replies);
                return closeAfterReply
                        ? CommandResult.closeAfterReply(aggregate)
                        : CommandResult.reply(aggregate);
            } catch (RuntimeException | Error failure) {
                Throwable primary = childExecutionStarted
                        ? resultUnknown(failure)
                        : failure;
                closeOwnedReverse(primary);
                rethrow(primary);
                throw new AssertionError("unreachable");
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

        private CommandResult executeChild(PreparedCommand child) {
            CommandSession childContext = session;
            return Objects.requireNonNull(
                    child.execute(childContext),
                    "transaction child returned null");
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            closeOwnedReverse(null);
        }

        private void closeOwnedReverse(Throwable primary) {
            // child 回复可能仍引用其 owner；按队列索引逆序，并在同一索引上先关 child 再归还 request。
            Throwable failure = primary;
            int count = Math.max(children.size(), drainedRequests.size());
            for (int index = count - 1; index >= 0; index--) {
                if (index < children.size()) {
                    PreparedCommand child = children.get(index);
                    if (child != null) {
                        failure = closeAccumulating(failure, child::close);
                    }
                }
                if (index < drainedRequests.size()) {
                    ExecutionRequest request = drainedRequests.get(index);
                    if (request != null) {
                        failure = closeAccumulating(failure, request::close);
                    }
                }
            }
            children.clear();
            drainedRequests = List.of();
            if (primary == null && failure != null) {
                rethrow(failure);
            }
        }

        private static Throwable closeAccumulating(Throwable primary, Runnable close) {
            try {
                close.run();
            } catch (RuntimeException | Error closeFailure) {
                if (primary == null) {
                    return closeFailure;
                }
                if (primary != closeFailure) {
                    primary.addSuppressed(closeFailure);
                }
            }
            return primary;
        }

        private static Throwable resultUnknown(Throwable failure) {
            if (failure instanceof ResultUnknownException) {
                return failure;
            }
            return new ResultUnknownException(
                    "transaction child result may already be visible", failure);
        }
    }
}
