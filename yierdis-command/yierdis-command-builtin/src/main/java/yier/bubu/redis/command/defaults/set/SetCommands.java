package yier.bubu.redis.command.defaults.set;

import java.util.List;
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
import yier.bubu.redis.command.defaults.CollectionScanCommandSupport;
import yier.bubu.redis.command.defaults.CommandSupport;
import yier.bubu.redis.command.defaults.DbReplies;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;

public final class SetCommands implements CommandModule {
    private static final CommandKeySpec KEY = new CommandKeySpec(1, 1, 1);

    private final CommandSupport support;

    public SetCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandSpec(syntax("SADD", CommandArity.min(3)), args -> change(args, true)));
        registration.register(new CommandSpec(syntax("SREM", CommandArity.min(3)), args -> change(args, false)));
        registration.register(new CommandSpec(syntax("SMEMBERS", CommandArity.exact(2)), this::smembers));
        registration.register(new CommandSpec(syntax("SISMEMBER", CommandArity.exact(3)), this::sismember));
        registration.register(new CommandSpec(syntax("SCARD", CommandArity.exact(2)), this::scard));
        registration.register(new CommandSpec(syntax("SSCAN", CommandArity.min(3)), this::sscan));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity) {
        return new CommandSyntax(nameUpper, arity, KEY, TransactionPolicy.QUEUEABLE);
    }

    private CommandInvocation change(CommandArgs args, boolean add) {
        byte[] key = args.bytes(1);
        List<byte[]> members = args.byteArraysFrom(2);
        return session -> PreparedCommands.action(ReplyShapes.integerUpperBound(), execution -> {
            try {
                long changed = (add
                        ? support.commandDb(execution).writes().sets().sadd(key, members)
                        : support.commandDb(execution).writes().sets().srem(key, members)).value();
                return CommandResult.reply(RedisReplies.integer(changed));
            } catch (WrongTypeException | YierdisCommandException failure) {
                return CommandResult.controlError(failure.getMessage());
            }
        });
    }

    private CommandInvocation smembers(CommandArgs args) {
        byte[] key = args.bytes(1);
        return session -> {
            ByteSequenceSource values = support.commandDb(session).reads().sets().smembers(key);
            RedisReply reply = DbReplies.set(values);
            return PreparedCommands.owned(CommandResult.reply(reply), values);
        };
    }

    private CommandInvocation sismember(CommandArgs args) {
        byte[] key = args.bytes(1);
        byte[] member = args.bytes(2);
        return session -> PreparedCommands.ready(RedisReplies.integer(
                support.commandDb(session).reads().sets().sismember(key, member) ? 1L : 0L));
    }

    private CommandInvocation scard(CommandArgs args) {
        byte[] key = args.bytes(1);
        return session -> PreparedCommands.ready(RedisReplies.integer(
                support.commandDb(session).reads().sets().scard(key)));
    }

    private CommandInvocation sscan(CommandArgs args) throws CommandParseException {
        CollectionScanCommandSupport.Arguments parsed = CollectionScanCommandSupport.parse(args, false);
        return session -> CollectionScanCommandSupport.prepareReply(
                support.commandDb(session).reads().sets().sscan(
                        parsed.key(), parsed.cursor(), parsed.match(), parsed.count()
                ));
    }
}
