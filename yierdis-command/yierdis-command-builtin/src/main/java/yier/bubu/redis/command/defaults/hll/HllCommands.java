package yier.bubu.redis.command.defaults.hll;

import java.util.List;
import java.util.Objects;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandInvocation;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.defaults.CommandSupport;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.YierdisCommandException;

public final class HllCommands implements CommandModule {
    private static final CommandKeySpec KEY = new CommandKeySpec(1, 1, 1);
    private static final CommandKeySpec MULTI_KEYS = new CommandKeySpec(1, -1, 1);

    private final CommandSupport support;

    public HllCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandSpec(syntax("PFADD", CommandArity.min(3), KEY), this::pfadd));
        registration.register(new CommandSpec(syntax("PFCOUNT", CommandArity.min(2), MULTI_KEYS), this::pfcount));
        registration.register(new CommandSpec(syntax("PFMERGE", CommandArity.min(3), MULTI_KEYS), this::pfmerge));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity, CommandKeySpec keys) {
        return new CommandSyntax(nameUpper, arity, keys, TransactionPolicy.QUEUEABLE);
    }

    private CommandInvocation pfadd(CommandArgs args) {
        byte[] key = args.bytes(1);
        List<byte[]> elements = args.byteArraysFrom(2);
        return session -> PreparedCommands.action(ReplyShapes.integerUpperBound(), execution -> {
            try {
                long changed = support.commandDb(execution).writes().hll().pfadd(key, elements).value();
                return CommandResult.reply(RedisReplies.integer(changed));
            } catch (WrongTypeException | YierdisCommandException failure) {
                return CommandResult.controlError(failure.getMessage());
            }
        });
    }

    private CommandInvocation pfcount(CommandArgs args) {
        List<byte[]> keys = args.byteArraysFrom(1);
        return session -> PreparedCommands.ready(RedisReplies.integer(
                support.commandDb(session).reads().hll().pfcount(keys)));
    }

    private CommandInvocation pfmerge(CommandArgs args) {
        byte[] destination = args.bytes(1);
        List<byte[]> sources = args.byteArraysFrom(2);
        return session -> PreparedCommands.action(ReplyShapes.simpleString("OK"), execution -> {
            try {
                support.commandDb(execution).writes().hll().pfmerge(destination, sources);
                return CommandResult.reply(RedisReplies.simpleString("OK"));
            } catch (WrongTypeException | YierdisCommandException failure) {
                return CommandResult.controlError(failure.getMessage());
            }
        });
    }
}
