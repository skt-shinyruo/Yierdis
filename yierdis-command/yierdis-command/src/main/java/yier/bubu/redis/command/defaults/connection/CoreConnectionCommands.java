package yier.bubu.redis.command.defaults.connection;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandArity;
import java.util.function.Function;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParseException;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.defaults.CommandSupport;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.execution.api.ReplyShapes;

/** Transport-neutral connection and DB lifecycle commands. */
public final class CoreConnectionCommands {
    private final CommandSupport support;

    public CoreConnectionCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandSpec(
                syntax("PING", CommandArity.oneOf(1, 2)), this::ping));
        registration.register(new CommandSpec(
                syntax("ECHO", CommandArity.exact(2)), this::echo));
        registration.register(new CommandSpec(
                syntax("COMMAND", CommandArity.min(1)),
                args -> command(args, registration)));
        registration.register(new CommandSpec(
                syntax("SELECT", CommandArity.exact(2)), this::select));
        registration.register(new CommandSpec(
                syntax("QUIT", CommandArity.exact(1)), this::quit));
        registration.register(new CommandSpec(
                syntax("CLIENT", CommandArity.min(2)), this::client));
        registration.register(new CommandSpec(
                syntax("AUTH", CommandArity.min(2)), this::auth));
        registration.register(new CommandSpec(
                syntax("FLUSHDB", CommandArity.oneOf(1, 2)), this::flushdb));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity) {
        return new CommandSyntax(nameUpper, arity, CommandKeySpec.NONE, TransactionPolicy.QUEUEABLE);
    }

    private Function<CommandSession, PreparedCommand> ping(CommandArgs args) {
        if (args.argc() == 1) {
            return session -> PreparedCommands.ready(RedisReplies.simpleString("PONG"));
        }
        ExecutionRequest request = args.request();
        return session -> retainedArgument(request, 1);
    }

    private Function<CommandSession, PreparedCommand> echo(CommandArgs args) {
        ExecutionRequest request = args.request();
        return session -> retainedArgument(request, 1);
    }

    private Function<CommandSession, PreparedCommand> select(CommandArgs args) {
        int dbIndex = args.intClampedAt(1);
        return session -> {
            if (dbIndex < 0 || dbIndex >= support.databases()) {
                return error("ERR DB index is out of range");
            }
            return PreparedCommands.action(
                    ReplyShapes.simpleString("OK"),
                    execution -> {
                        execution.setDbIndex(dbIndex);
                        return CommandResult.reply(RedisReplies.simpleString("OK"));
                    }
            );
        };
    }

    private Function<CommandSession, PreparedCommand> quit(CommandArgs args) {
        return session -> PreparedCommands.ready(
                CommandResult.closeAfterReply(RedisReplies.simpleString("OK")));
    }

    private Function<CommandSession, PreparedCommand> client(CommandArgs args) {
        if (args.is(1, "SETINFO")) {
            return session -> ok();
        }
        if (args.is(1, "SETNAME")) {
            if (args.argc() != 3) {
                throw new CommandParseException(
                        "ERR wrong number of arguments for 'client|setname' command");
            }
            String name = args.utf8(2);
            return session -> PreparedCommands.action(
                    ReplyShapes.simpleString("OK"),
                    execution -> {
                        execution.setClientName(name);
                        return CommandResult.reply(RedisReplies.simpleString("OK"));
                    }
            );
        }
        if (args.is(1, "GETNAME")) {
            if (args.argc() != 2) {
                throw new CommandParseException(
                        "ERR wrong number of arguments for 'client|getname' command");
            }
            return session -> {
                String name = session.clientName();
                return name == null
                        ? PreparedCommands.ready(RedisReplies.nullValue())
                        : PreparedCommands.ready(
                                RedisReplies.bulkString(name.getBytes(StandardCharsets.UTF_8)));
            };
        }
        throw new CommandParseException(
                "ERR unknown subcommand '" + args.utf8(1) + "'. Try CLIENT HELP.");
    }

    private Function<CommandSession, PreparedCommand> auth(CommandArgs args) {
        return session -> error(
                "ERR AUTH <password> called without any password configured for the default user. "
                        + "Are you sure your configuration is correct?"
        );
    }

    private Function<CommandSession, PreparedCommand> flushdb(CommandArgs args) {
        if (args.argc() == 2 && !args.is(1, "SYNC") && !args.is(1, "ASYNC")) {
            throw new CommandParseException("ERR syntax error");
        }
        boolean async = args.argc() == 2 && args.is(1, "ASYNC");
        return session -> PreparedCommands.action(
                ReplyShapes.simpleString("OK"),
                execution -> {
                    var db = support.commandDb(execution);
                    if (async) {
                        db.flushDbAsync();
                    } else {
                        db.flushDb();
                    }
                    return CommandResult.reply(RedisReplies.simpleString("OK"));
                }
        );
    }

    private static PreparedCommand retainedArgument(ExecutionRequest request, int index) {
        ExecutionRequest retained = request.retain();
        try {
            RedisReply reply = retained.isNull(index)
                    ? RedisReplies.nullValue()
                    : RedisReplies.bulkString(
                            retained.len(index),
                            retained.admittedMemoryBytes(),
                            sink -> sink.bulkString(retained.readOnlyByteArray(index))
                    );
            return PreparedCommands.owned(CommandResult.reply(reply), retained);
        } catch (RuntimeException | Error failure) {
            try {
                retained.close();
            } catch (RuntimeException | Error closeFailure) {
                if (closeFailure != failure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            throw failure;
        }
    }

    private static Function<CommandSession, PreparedCommand> command(
            CommandArgs args,
            CommandModule.Registration registration
    ) {
        if (args.argc() == 1) {
            return session -> {
                ArrayList<RedisReply> infos = new ArrayList<>();
                for (String name : registration.upperNamesSorted()) {
                    infos.add(commandInfo(registration, name));
                }
                return PreparedCommands.ready(RedisReplies.array(infos));
            };
        }
        if (args.argc() == 2 && args.is(1, "COUNT")) {
            return session -> PreparedCommands.ready(
                    RedisReplies.integer(registration.commandCount()));
        }
        if (args.argc() >= 2 && args.is(1, "INFO")) {
            if (args.argc() == 2) {
                throw new CommandParseException(
                        "ERR wrong number of arguments for 'command' command");
            }
            ArrayList<String> names = new ArrayList<>(args.argc() - 2);
            for (int index = 2; index < args.argc(); index++) {
                names.add(commandInfoName(args, index));
            }
            return session -> {
                ArrayList<RedisReply> infos = new ArrayList<>(names.size());
                for (String name : names) {
                    infos.add(commandInfo(registration, name));
                }
                return PreparedCommands.ready(RedisReplies.array(infos));
            };
        }
        throw new CommandParseException("ERR syntax error");
    }

    private static RedisReply commandInfo(
            CommandModule.Registration registration,
            String nameUpper
    ) {
        CommandSpec spec = nameUpper == null ? null : registration.specByUpperName(nameUpper);
        if (spec == null) {
            return RedisReplies.nullArray();
        }
        CommandSyntax syntax = spec.syntax();
        return RedisReplies.array(List.of(
                RedisReplies.bulkString(syntax.nameLower().getBytes(StandardCharsets.US_ASCII)),
                RedisReplies.integer(syntax.arity().redisMetadataArity()),
                RedisReplies.array(List.of()),
                RedisReplies.integer(syntax.keys().firstKeyIndex()),
                RedisReplies.integer(syntax.keys().lastKeyIndex()),
                RedisReplies.integer(syntax.keys().keyStep())
        ));
    }

    private static String commandInfoName(CommandArgs args, int index) {
        if (args.isNull(index) || args.length(index) <= 0) {
            return null;
        }
        String upper = args.utf8(index);
        return upper == null || upper.isBlank()
                ? null
                : upper.trim().toUpperCase(Locale.ROOT);
    }

    private static PreparedCommand ok() {
        return PreparedCommands.ready(RedisReplies.simpleString("OK"));
    }

    private static PreparedCommand error(String message) {
        return PreparedCommands.ready(CommandResult.error(message));
    }

}
