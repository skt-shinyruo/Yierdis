package yier.bubu.redis.command.kernel;

import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandParseException;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.RedisArgEcho;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.ReplyAdmissionRequirement;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.TransactionState;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.YierdisCommandException;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.Function;

public final class CommandDispatcher {
    private static final String EMPTY_COMMAND = "ERR empty command";
    private static final String NULL_BULK_STRING = "ERR Protocol error: null bulk string";

    private final CommandRegistry registry;

    public CommandDispatcher(CommandRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    public PreparedCommand prepare(CommandSession session, ExecutionRequest request) {
        return prepare(session, request, true);
    }

    /**
     * 按已注册命令名返回 reply admission 约束；空、非 ASCII 或未注册名称保持普通流水线语义。
     *
     * <p>该查询不校验 arity；已识别命令即使参数数量错误，仍返回其注册约束。</p>
     */
    public ReplyAdmissionRequirement replyAdmissionRequirement(ExecutionRequest request) {
        Objects.requireNonNull(request, "request");
        if (request.argc() <= 0 || request.isNull(0) || request.len(0) <= 0) {
            return ReplyAdmissionRequirement.PIPELINED;
        }
        CommandSpec spec = registry.specByExactUpperName(exactUpperAsciiName(request));
        return spec == null
                ? ReplyAdmissionRequirement.PIPELINED
                : spec.syntax().replyAdmissionRequirement();
    }

    PreparedCommand prepareExecReplay(CommandSession session, ExecutionRequest request) {
        return prepare(session, request, false);
    }

    private PreparedCommand prepare(
            CommandSession session,
            ExecutionRequest request,
            boolean applyTransactionPolicy
    ) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(request, "request");

        int argc = request.argc();
        // Redis 只有“没有命令名数组”才算协议级空命令；长度为 0 的 bulk（含仅 NUL 截断后的空名）
        // 走 unknown command ''，这样 MULTI 里也会 EXECABORT 并带上 args beginning with。
        if (argc <= 0 || request.isNull(0)) {
            return abortingError(session, EMPTY_COMMAND);
        }

        String nameUpper = exactUpperAsciiName(request);
        if (hasIllegalNullArgument(request, nameUpper)) {
            return abortingError(session, NULL_BULK_STRING);
        }

        CommandSpec spec = nameUpper == null || nameUpper.isEmpty()
                ? null
                : registry.specByExactUpperName(nameUpper);
        if (spec == null) {
            return abortingError(session, unknownCommandMessage(request));
        }

        CommandArgs args = new CommandArgs(request);
        try {
            spec.syntax().arity().validate(spec.syntax().nameLower(), args);
        } catch (CommandParseException failure) {
            // 对齐 Redis rejectCommand：被拒的是 EXEC 本身时不只是标记 dirty，而是立刻丢弃队列并退出 MULTI。
            if (TransactionCommands.EXEC.equals(spec.syntax().nameUpper())) {
                return TransactionCommands.prepareRejectedExec(session, failure.getMessage());
            }
            return abortingError(session, failure.getMessage());
        }
        try {
            TransactionState transaction = session.transaction();
            if (applyTransactionPolicy && transaction.active()) {
                TransactionPolicy policy = spec.syntax().transactionPolicy();
                if (policy == TransactionPolicy.DISALLOWED_IN_MULTI) {
                    return abortingError(
                            session,
                            "ERR " + spec.syntax().nameUpper() + " is not allowed in MULTI"
                    );
                }
                if (policy == TransactionPolicy.QUEUEABLE) {
                    preflightMultiQueue(spec, args);
                    return prepareRetainedRequestEnqueue(transaction, request);
                }
            }

            Function<CommandSession, PreparedCommand> invocation = Objects.requireNonNull(
                    spec.handler().parse(args),
                    "command handler returned null"
            );
            return Objects.requireNonNull(
                    invocation.apply(session),
                    "command invocation returned null"
            );
        } catch (CommandParseException failure) {
            return abortingError(session, failure.getMessage());
        } catch (WrongTypeException | YierdisCommandException failure) {
            return error(failure.getMessage());
        }
    }

    private static void preflightMultiQueue(CommandSpec spec, CommandArgs args) {
        try {
            Function<CommandSession, PreparedCommand> deferredPrepare = spec.handler().parse(args);
            // EXEC replay 会重新 parse 并应用届时的 session；这里只确认 parse 能产出延迟 prepare。
            Objects.requireNonNull(deferredPrepare, "command handler returned null");
        } catch (CommandParseException failure) {
            // 入队只拒绝参数个数和未知子命令（与 Redis 容器命令 lookup 失败一致）。
            // 选项、取值和语法错误先 QUEUED，EXEC 时只有这一条失败。
            if (abortsMultiOnParse(failure.getMessage())) {
                throw failure;
            }
        }
    }

    private static boolean abortsMultiOnParse(String message) {
        return message != null
                && (message.contains("wrong number of arguments")
                || message.contains("unknown subcommand"));
    }

    private static PreparedCommand prepareRetainedRequestEnqueue(
            TransactionState transaction,
            ExecutionRequest request
    ) {
        return PreparedCommands.action(
                ReplyShapes.errorUpperBound(),
                context -> {
                    String enqueueError = transaction.tryEnqueue(request);
                    return enqueueError == null
                            ? CommandResult.reply(RedisReplies.simpleString("QUEUED"))
                            : CommandResult.error(enqueueError);
                }
        );
    }

    private static PreparedCommand abortingError(CommandSession session, String message) {
        return PreparedCommands.action(
                ReplyShapes.error(message),
                context -> {
                    TransactionState transaction = session.transaction();
                    if (transaction.active()) {
                        transaction.markAborted();
                    }
                    return CommandResult.error(message);
                }
        );
    }

    private static PreparedCommand error(String message) {
        return PreparedCommands.ready(
                RedisReplies.error(message)
        );
    }

    private static boolean hasIllegalNullArgument(ExecutionRequest request, String nameUpper) {
        int argc = request.argc();
        boolean allowNullMessage = argc == 2
                && ("PING".equals(nameUpper) || "ECHO".equals(nameUpper));
        for (int index = 1; index < argc; index++) {
            if (request.isNull(index) && !(allowNullMessage && index == 1)) {
                return true;
            }
        }
        return false;
    }

    private static String exactUpperAsciiName(ExecutionRequest request) {
        int length = request.len(0);
        byte[] upper = new byte[length];
        for (int index = 0; index < length; index++) {
            int value = request.byteAt(0, index) & 0xff;
            if (value > 0x7f) {
                return null;
            }
            if (value >= 'a' && value <= 'z') {
                value -= 'a' - 'A';
            }
            upper[index] = (byte) value;
        }
        return new String(upper, StandardCharsets.US_ASCII);
    }

    // 对齐 Redis commandCheckArity 的 unknown-command 文案：命令名 %.128s；若有参数再追加
    // ", with args beginning with: " 与逐个 "'%.*s' "（累计 128 字节预算）；回显规则见 RedisArgEcho。
    private static String unknownCommandMessage(ExecutionRequest request) {
        StringBuilder message = new StringBuilder("ERR unknown command '");
        RedisArgEcho.append(message, request, 0, 128);
        message.append('\'');
        if (request.argc() >= 2) {
            message.append(", with args beginning with: ");
            int argsBytes = 0;
            for (int index = 1; index < request.argc() && argsBytes < 128; index++) {
                int budget = 128 - argsBytes;
                message.append('\'');
                int written = RedisArgEcho.append(message, request, index, budget);
                message.append("' ");
                argsBytes += 1 + written + 2;
            }
        }
        return message.toString();
    }
}
