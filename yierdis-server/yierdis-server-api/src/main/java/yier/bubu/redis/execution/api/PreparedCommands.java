package yier.bubu.redis.execution.api;

import java.util.Objects;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.function.Supplier;

public final class PreparedCommands {
    private PreparedCommands() {
    }

    public static PreparedCommand ready(RedisReply reply) {
        return ready(CommandResult.reply(Objects.requireNonNull(reply, "reply")));
    }

    public static PreparedCommand ready(CommandResult result) {
        CommandResult readyResult = Objects.requireNonNull(result, "result");
        return create(
                readyResult.reply().shape(),
                OptionalInt.empty(),
                null,
                () -> ValidationResult.VALID,
                context -> readyResult);
    }

    public static PreparedCommand action(
            ReplyShape reservationShape,
            Function<CommandSession, CommandResult> action
    ) {
        return create(
                reservationShape,
                OptionalInt.empty(),
                null,
                () -> ValidationResult.VALID,
                action);
    }

    public static PreparedCommand action(
            ReplyShape reservationShape,
            int replyProtocolVersion,
            Function<CommandSession, CommandResult> action
    ) {
        return create(
                reservationShape,
                OptionalInt.of(replyProtocolVersion),
                null,
                () -> ValidationResult.VALID,
                action);
    }

    public static PreparedCommand owned(CommandResult result, AutoCloseable owner) {
        CommandResult readyResult = Objects.requireNonNull(result, "result");
        return ownedAction(
                readyResult.reply().shape(),
                owner,
                () -> ValidationResult.VALID,
                context -> readyResult);
    }

    public static PreparedCommand ownedAction(
            ReplyShape reservationShape,
            AutoCloseable owner,
            Supplier<ValidationResult> validation,
            Function<CommandSession, CommandResult> action
    ) {
        return create(
                reservationShape,
                OptionalInt.empty(),
                Objects.requireNonNull(owner, "owner"),
                validation,
                action);
    }

    private static PreparedCommand create(
            ReplyShape reservationShape,
            OptionalInt replyProtocolVersion,
            AutoCloseable owner,
            Supplier<ValidationResult> validation,
            Function<CommandSession, CommandResult> action
    ) {
        ReplyShape shape = Objects.requireNonNull(reservationShape, "reservationShape");
        OptionalInt declaredVersion = Objects.requireNonNull(replyProtocolVersion, "replyProtocolVersion");
        Supplier<ValidationResult> validator = Objects.requireNonNull(validation, "validation");
        Function<CommandSession, CommandResult> execution =
                Objects.requireNonNull(action, "action");
        return new PreparedCommand() {
            private AutoCloseable owned = owner;

            @Override
            public ReplyShape reservationShape() {
                return shape;
            }

            @Override
            public OptionalInt replyProtocolVersion() {
                return declaredVersion;
            }

            @Override
            public ValidationResult validateBeforeExecute() {
                return Objects.requireNonNull(
                        validator.get(),
                        "validation returned null");
            }

            @Override
            public CommandResult execute(CommandSession context) {
                CommandSession executionContext =
                        Objects.requireNonNull(context, "context");
                return Objects.requireNonNull(
                        execution.apply(executionContext),
                        "action returned null");
            }

            @Override
            public void close() {
                AutoCloseable closeable = owned;
                if (closeable == null) {
                    return;
                }
                owned = null;
                try {
                    closeable.close();
                } catch (RuntimeException failure) {
                    throw failure;
                } catch (Exception failure) {
                    throw new IllegalStateException(
                            "prepared command owner close failed",
                            failure);
                }
            }
        };
    }
}
