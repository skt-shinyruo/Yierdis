package yier.bubu.redis.command.kernel;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
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
import yier.bubu.redis.execution.api.ReplyAdmissionRequirement;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.execution.api.ValidationResult;

/** 事务控制命令以及 EXEC 的延迟回复所有权边界。 */
final class TransactionCommands {
    static final String EXEC = "EXEC";

    private static final String EXEC_ABORT = "EXECABORT Transaction discarded because of previous errors.";
    private static final String EXEC_REJECTED_PREFIX = "EXECABORT Transaction discarded because of: ";
    private static final String ERR_PREFIX = "ERR ";

    private final CommandDispatcher dispatcher;

    TransactionCommands(CommandDispatcher dispatcher) {
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    }

    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandSpec(syntax("MULTI"), this::multi));
        registration.register(new CommandSpec(syntax("DISCARD"), this::discard));
        registration.register(new CommandSpec(
                syntax(EXEC, ReplyAdmissionRequirement.BARRIER_UNTIL_CLEANUP),
                this::exec
        ));
    }

    private static CommandSyntax syntax(String name) {
        return syntax(name, ReplyAdmissionRequirement.PIPELINED);
    }

    private static CommandSyntax syntax(String name, ReplyAdmissionRequirement replyAdmissionRequirement) {
        return new CommandSyntax(
                name,
                CommandArity.exact(1),
                CommandKeySpec.NONE,
                TransactionPolicy.TRANSACTION_CONTROL,
                replyAdmissionRequirement
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
        ReplyShape reservationShape = tx.size() == 0
                ? ReplyShapes.array(List.of())
                : ReplyShapes.maximum();
        return new PreparedExec(tx, dispatcher, session, reservationShape, session.respVersion());
    }

    /**
     * EXEC 自身在 dispatcher 被拒（目前只有参数个数不对）时的回复：不论是否在 MULTI 里都返回
     * {@code EXECABORT Transaction discarded because of: <原因>}，执行时丢弃排队并回到普通模式。
     */
    static PreparedCommand prepareRejectedExec(CommandSession session, String reason) {
        // Redis 的 rejectCommandFormat 原因文本不带 "ERR "；这里的 arity 文案带前缀，拼接前去掉以保持同一字节形态。
        String detail = reason.startsWith(ERR_PREFIX) ? reason.substring(ERR_PREFIX.length()) : reason;
        String message = EXEC_REJECTED_PREFIX + detail;
        return PreparedCommands.action(
                ReplyShapes.error(message),
                context -> {
                    TransactionState tx = session.transaction();
                    if (tx.active()) {
                        tx.discard();
                    }
                    return CommandResult.error(message);
                }
        );
    }

    private static PreparedCommand error(String message) {
        return PreparedCommands.ready(RedisReplies.error(message));
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
            // prepareExecReplay 已转移 child 所有权；发布到清理列表失败时必须在当前栈帧归还。
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
        private final int replyProtocolVersion;
        private List<ExecutionRequest> drainedRequests = List.of();
        private boolean executed;
        private boolean closed;

        private PreparedExec(
                TransactionState tx,
                CommandDispatcher dispatcher,
                CommandSession session,
                ReplyShape reservationShape,
                int replyProtocolVersion
        ) {
            this.tx = Objects.requireNonNull(tx, "tx");
            this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
            this.session = Objects.requireNonNull(session, "session");
            this.children = new ArrayList<>();
            this.reservationShape = Objects.requireNonNull(reservationShape, "reservationShape");
            this.replyProtocolVersion = replyProtocolVersion;
        }

        @Override
        public ReplyShape reservationShape() {
            return reservationShape;
        }

        // 显式声明外层版本，execute 里才能确定哪些 child 回复需要按另一版本编码。
        @Override
        public OptionalInt replyProtocolVersion() {
            return OptionalInt.of(replyProtocolVersion);
        }

        @Override
        public ValidationResult validateBeforeExecute() {
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
                ArrayList<RedisReply> replies = new ArrayList<>(queued.size());
                boolean closeAfterReply = false;
                for (int index = 0; index < queued.size(); index++) {
                    ExecutionRequest request = queued.get(index);
                    // 按队列顺序准备 child，使前一命令的 session/DB 副作用对后一命令可见。
                    PreparedCommand child = prepareCurrentChild(request);
                    addOwnedChild(children, child);
                    childExecutionStarted = true;
                    CommandResult result = executeChild(child);
                    RedisReply reply = result.reply();
                    if (reply instanceof RedisReply.ControlError controlError) {
                        reply = RedisReplies.error(controlError.message());
                    }
                    // 对齐 Redis：排队的 HELLO 在执行时切换协议，它自己和之后的 child 回复按新版本编码，
                    // 之前的仍按外层版本。只有空队列才用精确预留，而切换版本至少需要一个 child，
                    // 所以这里一定处在 maximum 预留下。
                    int childProtocolVersion = session.respVersion();
                    if (childProtocolVersion != replyProtocolVersion) {
                        reply = RedisReplies.protocolVersioned(childProtocolVersion, reply);
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
                PreparedCommand child = dispatcher.prepareExecReplay(session, request);
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
