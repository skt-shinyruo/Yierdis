package yier.bubu.redis.command.defaults.set;

import java.util.List;
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
import yier.bubu.redis.command.defaults.CollectionScanCommandSupport;
import yier.bubu.redis.command.defaults.CommandSupport;
import yier.bubu.redis.command.defaults.DbReplies;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;

public final class SetCommands {
    private static final CommandKeySpec KEY = new CommandKeySpec(1, 1, 1);

    private final CommandSupport support;

    public SetCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandSpec(syntax("SADD", CommandArity.min(3)),
                args -> change(args, SetChange.ADD)));
        registration.register(new CommandSpec(syntax("SREM", CommandArity.min(3)),
                args -> change(args, SetChange.REMOVE)));
        registration.register(new CommandSpec(syntax("SMEMBERS", CommandArity.exact(2)), this::smembers));
        registration.register(new CommandSpec(syntax("SISMEMBER", CommandArity.exact(3)), this::sismember));
        registration.register(new CommandSpec(syntax("SCARD", CommandArity.exact(2)), this::scard));
        registration.register(new CommandSpec(syntax("SSCAN", CommandArity.min(3)), this::sscan));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity) {
        return new CommandSyntax(nameUpper, arity, KEY, TransactionPolicy.QUEUEABLE);
    }

    private Function<CommandSession, PreparedCommand> change(CommandArgs args, SetChange change) {
        byte[] key = args.bytes(1);
        List<byte[]> members = args.byteArraysFrom(2);
        return session -> CommandSupport.preparedAction(ReplyShapes.integerUpperBound(), execution -> {
            long changed = (change == SetChange.ADD
                    ? support.commandDb(execution).sets().sadd(key, members)
                    : support.commandDb(execution).sets().srem(key, members)).value();
            return CommandResult.reply(RedisReplies.integer(changed));
        });
    }

    private Function<CommandSession, PreparedCommand> smembers(CommandArgs args) {
        byte[] key = args.bytes(1);
        return session -> {
            ByteSequenceSource values = support.commandDb(session).sets().smembers(key);
            RedisReply reply = DbReplies.set(values);
            return PreparedCommands.owned(CommandResult.reply(reply), values);
        };
    }

    private Function<CommandSession, PreparedCommand> sismember(CommandArgs args) {
        byte[] key = args.bytes(1);
        byte[] member = args.bytes(2);
        return session -> PreparedCommands.ready(RedisReplies.integer(
                support.commandDb(session).sets().sismember(key, member) ? 1L : 0L));
    }

    private Function<CommandSession, PreparedCommand> scard(CommandArgs args) {
        byte[] key = args.bytes(1);
        return session -> PreparedCommands.ready(RedisReplies.integer(
                support.commandDb(session).sets().scard(key)));
    }

    private Function<CommandSession, PreparedCommand> sscan(CommandArgs args) {
        CollectionScanCommandSupport.Arguments parsed = CollectionScanCommandSupport.parse(args);
        return session -> CollectionScanCommandSupport.prepareReply(
                support.commandDb(session).sets().sscan(
                        parsed.key(), parsed.cursor(), parsed.match(), parsed.count()
                ));
    }

    private enum SetChange {
        ADD,
        REMOVE
    }
}
