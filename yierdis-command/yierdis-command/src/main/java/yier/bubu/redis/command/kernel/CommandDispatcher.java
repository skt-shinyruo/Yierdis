package yier.bubu.redis.command.kernel;

import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandParseException;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReplies;
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
        if (argc <= 0 || request.isNull(0) || request.len(0) == 0) {
            return abortingError(session, EMPTY_COMMAND);
        }

        String nameUpper = exactUpperAsciiName(request);
        if (hasIllegalNullArgument(request, nameUpper)) {
            return abortingError(session, NULL_BULK_STRING);
        }

        CommandSpec spec = registry.specByExactUpperName(nameUpper);
        if (spec == null) {
            return abortingError(session, unknownCommandMessage(request));
        }

        CommandArgs args = new CommandArgs(request);
        try {
            spec.syntax().arity().validate(spec.syntax().nameLower(), args);

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
        Function<CommandSession, PreparedCommand> deferredPrepare = spec.handler().parse(args);
        // EXEC replay 会重新 parse 并应用届时的 session；这里仅确认 preflight 产出了合法的延迟 prepare。
        Objects.requireNonNull(deferredPrepare, "command handler returned null");
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

    private static String unknownCommandMessage(ExecutionRequest request) {
        int length = request.len(0);
        if (length > 64) {
            return "ERR unknown command";
        }
        for (int index = 0; index < length; index++) {
            int value = request.byteAt(0, index) & 0xff;
            if (value < 0x20 || value > 0x7e || value == '\'' || value == '\\') {
                return "ERR unknown command";
            }
        }
        byte[] name = request.readOnlyByteArray(0);
        return "ERR unknown command '" + new String(name, StandardCharsets.US_ASCII) + "'";
    }
}
