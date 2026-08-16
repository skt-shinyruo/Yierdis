package yier.bubu.redis.command.defaults.list;

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
import yier.bubu.redis.command.defaults.CommandSupport;
import yier.bubu.redis.command.defaults.DbReplies;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.storage.api.PreparedMutation;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;

public final class ListCommands implements CommandModule {
    private static final String INTEGER_ERROR = "ERR value is not an integer or out of range";
    private static final CommandKeySpec KEY = new CommandKeySpec(1, 1, 1);

    private final CommandSupport support;

    public ListCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandSpec(syntax("LPUSH", CommandArity.min(3)), args -> push(args, true)));
        registration.register(new CommandSpec(syntax("RPUSH", CommandArity.min(3)), args -> push(args, false)));
        registration.register(new CommandSpec(syntax("LRANGE", CommandArity.exact(4)), this::lrange));
        registration.register(new CommandSpec(syntax("LPOP", CommandArity.oneOf(2, 3)), args -> pop(args, true)));
        registration.register(new CommandSpec(syntax("RPOP", CommandArity.oneOf(2, 3)), args -> pop(args, false)));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity) {
        return new CommandSyntax(nameUpper, arity, KEY, TransactionPolicy.QUEUEABLE);
    }

    private CommandInvocation push(CommandArgs args, boolean left) {
        byte[] key = args.bytes(1);
        List<byte[]> values = args.byteArraysFrom(2);
        return session -> PreparedCommands.action(ReplyShapes.integerUpperBound(), execution -> {
            try {
                long length = (left
                        ? support.commandDb(execution).writes().lists().lpush(key, values)
                        : support.commandDb(execution).writes().lists().rpush(key, values)).value();
                return CommandResult.reply(RedisReplies.integer(length));
            } catch (WrongTypeException | YierdisCommandException failure) {
                return CommandResult.controlError(failure.getMessage());
            }
        });
    }

    private CommandInvocation lrange(CommandArgs args) throws CommandParseException {
        byte[] key = args.bytes(1);
        int start = args.intClampedAt(2);
        int stop = args.intClampedAt(3);
        return session -> {
            ByteSequenceSource values = support.commandDb(session).reads().lists().lrange(key, start, stop);
            RedisReply reply = DbReplies.sequence(values);
            return PreparedCommands.owned(CommandResult.reply(reply), values);
        };
    }

    private CommandInvocation pop(CommandArgs args, boolean left) throws CommandParseException {
        boolean hasCount = args.argc() == 3;
        int count = 1;
        if (hasCount) {
            long parsed = args.longAt(2);
            if (parsed < 0L) {
                throw new CommandParseException(INTEGER_ERROR);
            }
            count = parsed > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) parsed;
        }
        PopArgs parsed = new PopArgs(args.bytes(1), count, hasCount, left);
        return session -> preparePop(parsed, session);
    }

    private PreparedCommand preparePop(
            PopArgs args,
            yier.bubu.redis.execution.api.CommandSession session
    ) {
        PreparedMutation<PoppedValueSequence> mutation = support.commandDb(session).writes().lists()
                .preparePop(args.key(), args.count(), args.left());
        PoppedValueSequence preview = mutation.preview();
        RedisReply reply;
        if (preview == null || preview.isNull()) {
            reply = args.hasCount() ? RedisReplies.nullArray() : RedisReplies.nullValue();
        } else if (args.hasCount()) {
            reply = DbReplies.sequence(preview);
        } else {
            reply = DbReplies.singleValue(preview);
        }
        return CommandSupport.preparedMutation(
                reply.shape(),
                mutation,
                execution -> {
                    mutation.commit();
                    return CommandResult.reply(reply);
                }
        );
    }

    private record PopArgs(byte[] key, int count, boolean hasCount, boolean left) {
    }
}
