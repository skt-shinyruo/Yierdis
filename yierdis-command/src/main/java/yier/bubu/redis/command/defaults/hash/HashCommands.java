package yier.bubu.redis.command.defaults.hash;

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
import yier.bubu.redis.storage.api.result.ByteMapSource;
import yier.bubu.redis.storage.api.result.ByteValue;

public final class HashCommands {
    private static final CommandKeySpec KEY = new CommandKeySpec(1, 1, 1);

    private final CommandSupport support;

    public HashCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandSpec(syntax("HSET", CommandArity.pairTail(4, 2)), this::hset));
        registration.register(new CommandSpec(syntax("HGET", CommandArity.exact(3)), this::hget));
        registration.register(new CommandSpec(syntax("HGETALL", CommandArity.exact(2)), this::hgetall));
        registration.register(new CommandSpec(syntax("HLEN", CommandArity.exact(2)), this::hlen));
        registration.register(new CommandSpec(syntax("HDEL", CommandArity.min(3)), this::hdel));
        registration.register(new CommandSpec(syntax("HSCAN", CommandArity.min(3)), this::hscan));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity) {
        return new CommandSyntax(nameUpper, arity, KEY, TransactionPolicy.QUEUEABLE);
    }

    private Function<CommandSession, PreparedCommand> hset(CommandArgs args) {
        byte[] key = args.bytes(1);
        List<byte[]> pairs = args.byteArraysFrom(2);
        return session -> CommandSupport.preparedAction(ReplyShapes.integerUpperBound(), execution -> {
            long added = support.commandDb(execution).hashes().hset(key, pairs).value();
            return CommandResult.reply(RedisReplies.integer(added));
        });
    }

    private Function<CommandSession, PreparedCommand> hget(CommandArgs args) {
        byte[] key = args.bytes(1);
        byte[] field = args.bytes(2);
        return session -> {
            ByteValue value = support.commandDb(session).hashes().hget(key, field);
            return PreparedCommands.owned(CommandResult.reply(DbReplies.value(value)), value);
        };
    }

    private Function<CommandSession, PreparedCommand> hgetall(CommandArgs args) {
        byte[] key = args.bytes(1);
        return session -> {
            ByteMapSource values = support.commandDb(session).hashes().hgetall(key);
            RedisReply reply = DbReplies.map(values);
            return PreparedCommands.owned(CommandResult.reply(reply), values);
        };
    }

    private Function<CommandSession, PreparedCommand> hlen(CommandArgs args) {
        byte[] key = args.bytes(1);
        return session -> PreparedCommands.ready(RedisReplies.integer(
                support.commandDb(session).hashes().hlen(key)));
    }

    private Function<CommandSession, PreparedCommand> hdel(CommandArgs args) {
        byte[] key = args.bytes(1);
        List<byte[]> fields = args.byteArraysFrom(2);
        return session -> CommandSupport.preparedAction(ReplyShapes.integerUpperBound(), execution -> {
            long deleted = support.commandDb(execution).hashes().hdel(key, fields).value();
            return CommandResult.reply(RedisReplies.integer(deleted));
        });
    }

    private Function<CommandSession, PreparedCommand> hscan(CommandArgs args) {
        CollectionScanCommandSupport.Arguments parsed =
                CollectionScanCommandSupport.parseWithOptionalNoValues(args);
        return session -> CollectionScanCommandSupport.prepareReply(
                support.commandDb(session).hashes().hscan(
                        parsed.key(), parsed.cursor(), parsed.match(), parsed.count(), parsed.noValues()
                ));
    }
}
