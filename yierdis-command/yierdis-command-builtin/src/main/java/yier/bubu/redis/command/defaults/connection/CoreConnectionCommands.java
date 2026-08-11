package yier.bubu.redis.command.defaults.connection;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandInvocation;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParseException;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.defaults.CommandSupport;
import yier.bubu.redis.execution.api.CommandResult;
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

    private CommandInvocation ping(CommandArgs args) {
        if (args.argc() == 1) {
            return session -> PreparedCommands.ready(RedisReplies.simpleString("PONG"));
        }
        ExecutionRequest request = args.request();
        return session -> retainedArgument(request, 1);
    }

    private CommandInvocation echo(CommandArgs args) {
        ExecutionRequest request = args.request();
        return session -> retainedArgument(request, 1);
    }

    private CommandInvocation select(CommandArgs args) throws CommandParseException {
        int dbIndex = args.intClampedAt(1);
        return session -> {
            if (dbIndex < 0 || dbIndex >= support.databases()) {
                return error("ERR DB index is out of range");
            }
            return PreparedCommands.action(
                    ReplyShapes.simpleString("OK"),
                    execution -> {
                        execution.session().setDbIndex(dbIndex);
                        return CommandResult.reply(RedisReplies.simpleString("OK"));
                    }
            );
        };
    }

    private CommandInvocation quit(CommandArgs args) {
        return session -> PreparedCommands.ready(
                CommandResult.closeAfterReply(RedisReplies.simpleString("OK")));
    }

    private CommandInvocation client(CommandArgs args) throws CommandParseException {
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
                        execution.session().setClientName(name);
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

    private CommandInvocation auth(CommandArgs args) {
        return session -> error(
                "ERR AUTH <password> called without any password configured for the default user. "
                        + "Are you sure your configuration is correct?"
        );
    }

    private CommandInvocation flushdb(CommandArgs args) throws CommandParseException {
        if (args.argc() == 2 && !args.is(1, "SYNC") && !args.is(1, "ASYNC")) {
            throw new CommandParseException("ERR syntax error");
        }
        boolean async = args.argc() == 2 && args.is(1, "ASYNC");
        return session -> PreparedCommands.action(
                ReplyShapes.simpleString("OK"),
                execution -> {
                    var lifecycle = support.commandDb(execution).lifecycle();
                    if (async) {
                        lifecycle.flushDbAsync();
                    } else {
                        lifecycle.flushDb();
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

    private static CommandInvocation command(
            CommandArgs args,
            CommandModule.Registration registration
    ) throws CommandParseException {
        if (args.argc() == 1) {
            return session -> {
                ArrayList<CommandInfo> infos = new ArrayList<>();
                for (String name : registration.upperNamesSorted()) {
                    infos.add(commandInfo(registration, name));
                }
                return commandInfos(infos);
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
            List<String> requestedNames = Collections.unmodifiableList(new ArrayList<>(names));
            return session -> {
                ArrayList<CommandInfo> infos = new ArrayList<>(requestedNames.size());
                for (String name : requestedNames) {
                    infos.add(commandInfo(registration, name));
                }
                return commandInfos(infos);
            };
        }
        throw new CommandParseException("ERR syntax error");
    }

    private static CommandInfo commandInfo(
            CommandModule.Registration registration,
            String nameUpper
    ) {
        CommandSpec spec = nameUpper == null ? null : registration.specByUpperName(nameUpper);
        return spec == null ? null : CommandInfo.of(spec.syntax());
    }

    private static PreparedCommand commandInfos(List<CommandInfo> infos) {
        ArrayList<RedisReply> replies = new ArrayList<>(infos.size());
        for (CommandInfo info : infos) {
            replies.add(info == null ? RedisReplies.nullArray() : info.toRedisReply());
        }
        return PreparedCommands.ready(RedisReplies.array(replies));
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

    private record CommandInfo(byte[] name, long arity, int firstKey, int lastKey, int keyStep) {
        private static CommandInfo of(CommandSyntax syntax) {
            return new CommandInfo(
                    syntax.nameLower().getBytes(StandardCharsets.US_ASCII),
                    syntax.arity().redisMetadataArity(),
                    syntax.keys().firstKeyIndex(),
                    syntax.keys().lastKeyIndex(),
                    syntax.keys().keyStep()
            );
        }

        private RedisReply toRedisReply() {
            return RedisReplies.array(List.of(
                    RedisReplies.bulkString(name),
                    RedisReplies.integer(arity),
                    RedisReplies.array(List.of()),
                    RedisReplies.integer(firstKey),
                    RedisReplies.integer(lastKey),
                    RedisReplies.integer(keyStep)
            ));
        }
    }
}
