package yier.bubu.redis.command.defaults.connection;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import yier.bubu.redis.command.api.ArgReader;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDefinition;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.defaults.CommandSupport;
import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;

/** Transport-neutral connection and DB lifecycle commands. */
public final class CoreConnectionCommands {
    private final CommandSupport support;

    public CoreConnectionCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandDefinition<>(syntax("PING", CommandArity.oneOf(1, 2)),
                CommandParsers.args(), this::ping));
        registration.register(new CommandDefinition<>(syntax("ECHO", CommandArity.exact(2)),
                CommandParsers.args(), this::echo));
        registration.register(new CommandDefinition<>(syntax("COMMAND", CommandArity.min(1)),
                CommandParsers.args(), (args, context) -> command(args, registration)));
        registration.register(new CommandDefinition<>(syntax("SELECT", CommandArity.exact(2)),
                CommandParsers.args(), this::select));
        registration.register(new CommandDefinition<>(syntax("QUIT", CommandArity.exact(1)),
                CommandParsers.args(), this::quit));
        registration.register(new CommandDefinition<>(syntax("CLIENT", CommandArity.min(2)),
                CommandParsers.args(), this::client));
        registration.register(new CommandDefinition<>(syntax("AUTH", CommandArity.min(2)),
                CommandParsers.args(), this::auth));
        registration.register(new CommandDefinition<>(syntax("FLUSHDB", CommandArity.oneOf(1, 2)),
                CommandParsers.args(), this::flushdb));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity) {
        return new CommandSyntax(nameUpper, arity, CommandKeySpec.NONE, TransactionPolicy.QUEUEABLE);
    }

    private PreparedCommand ping(ArgReader args, CommandPreparationContext context) {
        return args.argc() == 1
                ? CommandSupport.fixed(ReplyShapes.simpleString("PONG"), reply -> reply.reply().simpleString("PONG"))
                : retainedArgument(args.request(), 1);
    }

    private PreparedCommand echo(ArgReader args, CommandPreparationContext context) {
        return retainedArgument(args.request(), 1);
    }

    private PreparedCommand select(ArgReader args, CommandPreparationContext context) {
        final long parsed;
        try {
            parsed = args.longAt(1);
        } catch (IllegalArgumentException ignored) {
            return CommandSupport.error("ERR value is not an integer or out of range");
        }
        int dbIndex = parsed < Integer.MIN_VALUE ? Integer.MIN_VALUE
                : parsed > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) parsed;
        if (dbIndex < 0 || dbIndex >= support.databases()) {
            return CommandSupport.error("ERR DB index is out of range");
        }
        return CommandSupport.fixed(ReplyShapes.simpleString("OK"), execution -> {
            execution.session().setDbIndex(dbIndex);
            execution.reply().simpleString("OK");
        });
    }

    private PreparedCommand quit(ArgReader args, CommandPreparationContext context) {
        return CommandSupport.fixed(ReplyShapes.simpleString("OK"), execution -> {
            execution.reply().simpleString("OK");
            execution.reply().requestCloseAfterReply();
        });
    }

    private PreparedCommand client(ArgReader args, CommandPreparationContext context) {
        if (args.is(1, "SETINFO")) {
            return CommandSupport.fixed(ReplyShapes.simpleString("OK"), execution -> execution.reply().simpleString("OK"));
        }
        if (args.is(1, "SETNAME")) {
            if (args.argc() != 3) {
                return CommandSupport.error("ERR wrong number of arguments for 'client|setname' command");
            }
            String name = CommandSupport.utf8(args.bytes(2));
            return CommandSupport.fixed(ReplyShapes.simpleString("OK"), execution -> {
                execution.session().setClientName(name);
                execution.reply().simpleString("OK");
            });
        }
        if (args.is(1, "GETNAME")) {
            if (args.argc() != 2) {
                return CommandSupport.error("ERR wrong number of arguments for 'client|getname' command");
            }
            String name = context.session().clientName();
            if (name == null) {
                return CommandSupport.fixed(ReplyShapes.nullValue(), execution -> execution.reply().nullValue());
            }
            byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
            return CommandSupport.fixed(ReplyShapes.bulkString(bytes.length, 0L),
                    execution -> execution.reply().bulkString(bytes));
        }
        return CommandSupport.error("ERR unknown subcommand '" + CommandSupport.utf8(args.bytes(1))
                + "'. Try CLIENT HELP.");
    }

    private PreparedCommand auth(ArgReader args, CommandPreparationContext context) {
        return CommandSupport.error(
                "ERR AUTH <password> called without any password configured for the default user. "
                        + "Are you sure your configuration is correct?"
        );
    }

    private PreparedCommand flushdb(ArgReader args, CommandPreparationContext context) {
        if (args.argc() == 2 && !args.is(1, "SYNC") && !args.is(1, "ASYNC")) {
            return CommandSupport.error("ERR syntax error");
        }
        return CommandSupport.fixed(ReplyShapes.simpleString("OK"), execution -> {
            support.commandDb(execution).lifecycle().flushDb();
            execution.reply().simpleString("OK");
        });
    }

    private static PreparedCommand retainedArgument(ExecutionRequest request, int index) {
        ExecutionRequest retained = request.retain();
        ReplyShape shape = request.isNull(index)
                ? ReplyShapes.nullValue()
                : ReplyShapes.bulkString(request.len(index), retained.admittedMemoryBytes());
        return CommandSupport.owned(
                shape,
                retained,
                execution -> execution.reply().bulkString(request.readOnlyByteArray(index))
        );
    }

    private static PreparedCommand command(ArgReader args, CommandModule.Registration registration) {
        if (args.argc() == 1) {
            ArrayList<CommandInfo> infos = new ArrayList<>();
            for (String name : registration.upperNamesSorted()) {
                yier.bubu.redis.command.api.CommandDefinition<?> definition = registration.definitionByUpperName(name);
                infos.add(definition == null ? null : CommandInfo.of(definition.syntax()));
            }
            return commandInfos(infos);
        }
        if (args.argc() == 2 && args.is(1, "COUNT")) {
            int count = registration.commandCount();
            return CommandSupport.fixed(ReplyShapes.integer(count), execution -> execution.reply().integer(count));
        }
        if (args.argc() >= 2 && args.is(1, "INFO")) {
            if (args.argc() == 2) {
                return CommandSupport.error("ERR wrong number of arguments for 'command' command");
            }
            ArrayList<CommandInfo> infos = new ArrayList<>(args.argc() - 2);
            for (int index = 2; index < args.argc(); index++) {
                String upper = commandInfoName(args, index);
                yier.bubu.redis.command.api.CommandDefinition<?> definition = upper == null
                        ? null
                        : registration.definitionByUpperName(upper);
                infos.add(definition == null ? null : CommandInfo.of(definition.syntax()));
            }
            return commandInfos(infos);
        }
        return CommandSupport.error("ERR syntax error");
    }

    private static PreparedCommand commandInfos(List<CommandInfo> infos) {
        ArrayList<ReplyShape> shapes = new ArrayList<>(infos.size());
        for (CommandInfo info : infos) {
            shapes.add(info == null ? ReplyShapes.nullArray() : info.shape());
        }
        ReplyShape shape = ReplyShapes.array(shapes);
        return CommandSupport.fixed(shape, execution -> {
            execution.reply().arrayHeader(infos.size());
            for (CommandInfo info : infos) {
                if (info == null) {
                    execution.reply().nullArray();
                } else {
                    info.write(execution);
                }
            }
        });
    }

    private static String commandInfoName(ArgReader args, int index) {
        if (args.isNull(index) || args.len(index) <= 0) {
            return null;
        }
        String upper = CommandSupport.utf8(args.bytes(index));
        return upper == null || upper.isBlank() ? null : upper.trim().toUpperCase(Locale.ROOT);
    }

    private record CommandInfo(
            byte[] name,
            long arity,
            int firstKey,
            int lastKey,
            int keyStep,
            ReplyShape shape
    ) {
        private static CommandInfo of(CommandSyntax syntax) {
            byte[] name = syntax.nameLower().getBytes(StandardCharsets.US_ASCII);
            ReplyShape shape = ReplyShapes.array(List.of(
                    ReplyShapes.bulkString(name.length, 0L),
                    ReplyShapes.integer(syntax.arity().redisMetadataArity()),
                    ReplyShapes.array(List.of()),
                    ReplyShapes.integer(syntax.keys().firstKeyIndex()),
                    ReplyShapes.integer(syntax.keys().lastKeyIndex()),
                    ReplyShapes.integer(syntax.keys().keyStep())
            ));
            return new CommandInfo(
                    name,
                    syntax.arity().redisMetadataArity(),
                    syntax.keys().firstKeyIndex(),
                    syntax.keys().lastKeyIndex(),
                    syntax.keys().keyStep(),
                    shape
            );
        }

        private void write(yier.bubu.redis.execution.api.CommandExecutionContext context) {
            context.reply().arrayHeader(6);
            context.reply().bulkString(name);
            context.reply().integer(arity);
            context.reply().emptyArray();
            context.reply().integer(firstKey);
            context.reply().integer(lastKey);
            context.reply().integer(keyStep);
        }
    }
}
