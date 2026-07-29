package yier.bubu.redis.command.defaults.string;

import java.util.Objects;
import yier.bubu.redis.bytes.BytesSlice;
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
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.PreparedMutation;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.StringWriteOps;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.api.result.ByteValue;

public final class StringCommands implements CommandModule {
    private static final long MAX_STRING_BYTES = 512L * 1024 * 1024;
    private static final String SYNTAX_ERROR = "ERR syntax error";
    private static final String INVALID_SET_EXPIRE = "ERR invalid expire time in 'set' command";
    private static final String INVALID_BIT = "ERR bit is not an integer or out of range";
    private static final String STRING_TOO_LARGE = "ERR string exceeds maximum allowed size";
    private static final CommandKeySpec KEY = new CommandKeySpec(1, 1, 1);

    private final CommandSupport support;

    public StringCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandSpec(syntax("SET", CommandArity.min(3)), this::set));
        registration.register(new CommandSpec(syntax("GET", CommandArity.exact(2)), this::get));
        registration.register(new CommandSpec(syntax("STRLEN", CommandArity.exact(2)), this::strlen));
        registration.register(new CommandSpec(syntax("APPEND", CommandArity.exact(3)), this::append));
        registration.register(new CommandSpec(syntax("SETBIT", CommandArity.exact(4)), this::setbit));
        registration.register(new CommandSpec(syntax("GETBIT", CommandArity.exact(3)), this::getbit));
        registration.register(new CommandSpec(syntax("BITCOUNT", CommandArity.oneOf(2, 4)), this::bitcount));
        registration.register(new CommandSpec(syntax("INCR", CommandArity.exact(2)), args -> incrBy(args, 1L)));
        registration.register(new CommandSpec(syntax("DECR", CommandArity.exact(2)), args -> incrBy(args, -1L)));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity) {
        return new CommandSyntax(nameUpper, arity, KEY, TransactionPolicy.QUEUEABLE);
    }

    private record SetArgs(byte[] key, BytesSlice value, SetMode mode, ExpireOption expire, boolean getOld) {
    }

    private record SetBitArgs(byte[] key, long offset, int value) {
    }

    private record GetBitArgs(byte[] key, long offset) {
    }

    private record BitCountArgs(byte[] key, Long start, Long end) {
    }

    private CommandInvocation set(CommandArgs args) throws CommandParseException {
        byte[] key = args.bytes(1);
        SetMode mode = SetMode.NORMAL;
        ExpireOption expire = null;
        boolean getOld = false;
        for (int index = 3; index < args.argc(); index++) {
            if (args.is(index, "NX")) {
                if (mode != SetMode.NORMAL) {
                    throw syntaxFailure();
                }
                mode = SetMode.NX;
                continue;
            }
            if (args.is(index, "XX")) {
                if (mode != SetMode.NORMAL) {
                    throw syntaxFailure();
                }
                mode = SetMode.XX;
                continue;
            }
            if (args.is(index, "GET")) {
                if (getOld) {
                    throw syntaxFailure();
                }
                getOld = true;
                continue;
            }
            if (args.is(index, "KEEPTTL")) {
                if (expire != null) {
                    throw syntaxFailure();
                }
                expire = ExpireOption.keepTtl();
                continue;
            }
            if (!args.is(index, "EX") && !args.is(index, "PX")
                    && !args.is(index, "EXAT") && !args.is(index, "PXAT")) {
                throw syntaxFailure();
            }
            if (expire != null || index + 1 >= args.argc()) {
                throw syntaxFailure();
            }
            String option = args.utf8(index);
            long value = args.longAt(++index);
            if (value <= 0L) {
                throw new CommandParseException(INVALID_SET_EXPIRE);
            }
            if ("EX".equalsIgnoreCase(option)) {
                expire = ExpireOption.ex(value);
            } else if ("PX".equalsIgnoreCase(option)) {
                expire = ExpireOption.px(value);
            } else if ("EXAT".equalsIgnoreCase(option)) {
                long expireAtMillis;
                try {
                    expireAtMillis = Math.multiplyExact(value, 1000L);
                } catch (ArithmeticException ignored) {
                    expireAtMillis = Long.MAX_VALUE;
                }
                if (expireAtMillis <= System.currentTimeMillis()) {
                    throw new CommandParseException(INVALID_SET_EXPIRE);
                }
                expire = ExpireOption.exAt(value);
            } else {
                if (value <= System.currentTimeMillis()) {
                    throw new CommandParseException(INVALID_SET_EXPIRE);
                }
                expire = ExpireOption.pxAt(value);
            }
        }
        SetArgs parsed = new SetArgs(key, args.slice(2), mode, expire, getOld);
        return session -> prepareSet(parsed, session);
    }

    private PreparedCommand prepareSet(SetArgs args, yier.bubu.redis.execution.api.CommandSession session) {
        PreparedMutation<StringWriteOps.SetStringValue> mutation = support.commandDb(session).writes().strings()
                .prepareSet(args.key(), args.value(), args.mode(), args.expire(), args.getOld());
        StringWriteOps.SetStringValue preview = mutation.preview();
        RedisReply reply = args.getOld()
                ? DbReplies.value(preview.oldValue())
                : preview.applied() ? RedisReplies.simpleString("OK") : RedisReplies.nullValue();
        return CommandSupport.preparedMutation(
                reply.shape(), mutation,
                execution -> {
                    mutation.commit(execution.mutationContext());
                    return CommandResult.reply(reply);
                }
        );
    }

    private CommandInvocation get(CommandArgs args) {
        BytesSlice key = args.slice(1);
        return session -> {
            ByteValue value = support.commandDb(session).reads().strings().getStringValue(key);
            return PreparedCommands.owned(CommandResult.reply(DbReplies.value(value)), value);
        };
    }

    private CommandInvocation strlen(CommandArgs args) {
        BytesSlice key = args.slice(1);
        return session -> PreparedCommands.ready(RedisReplies.integer(
                support.commandDb(session).reads().strings().strlen(key)));
    }

    private CommandInvocation append(CommandArgs args) {
        byte[] key = args.bytes(1);
        BytesSlice value = args.slice(2);
        return session -> PreparedCommands.action(ReplyShapes.integerUpperBound(), execution -> {
            try {
                long length = support.commandDb(execution).writes().strings().append(key, value).value();
                return CommandResult.reply(RedisReplies.integer(length));
            } catch (WrongTypeException | YierdisCommandException failure) {
                return CommandResult.controlError(failure.getMessage());
            }
        });
    }

    private CommandInvocation setbit(CommandArgs args) throws CommandParseException {
        long offset = args.nonNegativeLongAt(2);
        long value;
        try {
            value = args.longAt(3);
        } catch (CommandParseException failure) {
            throw new CommandParseException(INVALID_BIT);
        }
        if (value != 0L && value != 1L) {
            throw new CommandParseException(INVALID_BIT);
        }
        if (offset / 8L >= MAX_STRING_BYTES) {
            throw new CommandParseException(STRING_TOO_LARGE);
        }
        SetBitArgs parsed = new SetBitArgs(args.bytes(1), offset, (int) value);
        return session -> PreparedCommands.action(ReplyShapes.integerUpperBound(), execution -> {
            try {
                int previous = support.commandDb(execution).writes().strings()
                        .setBit(parsed.key(), parsed.offset(), parsed.value()).value();
                return CommandResult.reply(RedisReplies.integer(previous));
            } catch (WrongTypeException | YierdisCommandException failure) {
                return CommandResult.controlError(failure.getMessage());
            }
        });
    }

    private CommandInvocation getbit(CommandArgs args) throws CommandParseException {
        GetBitArgs parsed = new GetBitArgs(args.bytes(1), args.nonNegativeLongAt(2));
        BytesSlice key = args.slice(1);
        return session -> PreparedCommands.ready(RedisReplies.integer(
                support.commandDb(session).reads().strings().getBit(key, parsed.offset())));
    }

    private CommandInvocation bitcount(CommandArgs args) throws CommandParseException {
        BitCountArgs parsed = args.argc() == 2
                ? new BitCountArgs(args.bytes(1), null, null)
                : new BitCountArgs(args.bytes(1), args.longAt(2), args.longAt(3));
        BytesSlice key = args.slice(1);
        return session -> {
            long count = parsed.start() == null
                    ? support.commandDb(session).reads().strings().bitcount(key)
                    : support.commandDb(session).reads().strings().bitcount(key, parsed.start(), parsed.end());
            return PreparedCommands.ready(RedisReplies.integer(count));
        };
    }

    private CommandInvocation incrBy(CommandArgs args, long delta) {
        byte[] key = args.bytes(1);
        return session -> PreparedCommands.action(ReplyShapes.integerUpperBound(), execution -> {
            try {
                long value = support.commandDb(execution).writes().strings().incrBy(key, delta).value();
                return CommandResult.reply(RedisReplies.integer(value));
            } catch (WrongTypeException | YierdisCommandException failure) {
                return CommandResult.controlError(failure.getMessage());
            }
        });
    }

    private static CommandParseException syntaxFailure() {
        return new CommandParseException(SYNTAX_ERROR);
    }
}
