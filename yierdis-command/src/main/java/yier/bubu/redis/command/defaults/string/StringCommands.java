package yier.bubu.redis.command.defaults.string;

import java.util.Objects;
import yier.bubu.redis.bytes.BytesSlice;
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
import yier.bubu.redis.command.defaults.DbReplies;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.storage.api.BitRangeUnit;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.PreparedMutation;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.StringOps;
import yier.bubu.redis.storage.api.result.ByteValue;

public final class StringCommands {
    private static final long MAX_STRING_BYTES = 512L * 1024 * 1024;
    private static final String SYNTAX_ERROR = "ERR syntax error";
    private static final String INVALID_SET_EXPIRE = "ERR invalid expire time in 'set' command";
    private static final String INVALID_BIT = "ERR bit is not an integer or out of range";
    private static final String INVALID_BIT_OFFSET = "ERR bit offset is not an integer or out of range";
    private static final CommandKeySpec KEY = new CommandKeySpec(1, 1, 1);

    private final CommandSupport support;

    public StringCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandSpec(syntax("SET", CommandArity.min(3)), this::set));
        registration.register(new CommandSpec(syntax("GET", CommandArity.exact(2)), this::get));
        registration.register(new CommandSpec(syntax("STRLEN", CommandArity.exact(2)), this::strlen));
        registration.register(new CommandSpec(syntax("APPEND", CommandArity.exact(3)), this::append));
        registration.register(new CommandSpec(syntax("SETBIT", CommandArity.exact(4)), this::setbit));
        registration.register(new CommandSpec(syntax("GETBIT", CommandArity.exact(3)), this::getbit));
        registration.register(new CommandSpec(syntax("BITCOUNT", CommandArity.min(2)), this::bitcount));
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

    private record BitCountArgs(byte[] key, Long start, Long end, BitRangeUnit unit) {
    }

    private Function<CommandSession, PreparedCommand> set(CommandArgs args) {
        byte[] key = args.bytes(1);
        SetMode mode = SetMode.NORMAL;
        ExpireOption expire = null;
        String expireKeyword = null;
        boolean getOld = false;
        for (int index = 3; index < args.argc(); index++) {
            // 与 Redis parseExtendedStringArgumentsOrReply 对齐：重复书写同一个 flag 是幂等的，
            // 只有互斥组合（NX/XX 混用、不同 expire 选项混用、expire 与 KEEPTTL 混用）才报 syntax error。
            if (args.is(index, "NX")) {
                if (mode == SetMode.XX) {
                    throw syntaxFailure();
                }
                mode = SetMode.NX;
                continue;
            }
            if (args.is(index, "XX")) {
                if (mode == SetMode.NX) {
                    throw syntaxFailure();
                }
                mode = SetMode.XX;
                continue;
            }
            if (args.is(index, "GET")) {
                getOld = true;
                continue;
            }
            if (args.is(index, "KEEPTTL")) {
                if (expireKeyword != null && !"KEEPTTL".equals(expireKeyword)) {
                    throw syntaxFailure();
                }
                expireKeyword = "KEEPTTL";
                expire = ExpireOption.keepTtl();
                continue;
            }
            String option;
            if (args.is(index, "EX")) {
                option = "EX";
            } else if (args.is(index, "PX")) {
                option = "PX";
            } else if (args.is(index, "EXAT")) {
                option = "EXAT";
            } else if (args.is(index, "PXAT")) {
                option = "PXAT";
            } else {
                throw syntaxFailure();
            }
            if (index + 1 >= args.argc()) {
                throw syntaxFailure();
            }
            if (expireKeyword != null && !expireKeyword.equals(option)) {
                throw syntaxFailure();
            }
            long value = args.longAt(++index);
            if (value <= 0L) {
                throw new CommandParseException(INVALID_SET_EXPIRE);
            }
            // 绝对过期的墙钟比较不在 parse：过去但为正的 EXAT/PXAT 在 execute 时先写后过期。
            if ("EX".equals(option)) {
                expire = ExpireOption.ex(value);
            } else if ("PX".equals(option)) {
                expire = ExpireOption.px(value);
            } else if ("EXAT".equals(option)) {
                expire = ExpireOption.exAt(value);
            } else {
                expire = ExpireOption.pxAt(value);
            }
            expireKeyword = option;
        }
        SetArgs parsed = new SetArgs(key, args.slice(2), mode, expire, getOld);
        return session -> prepareSet(parsed, session);
    }

    private PreparedCommand prepareSet(SetArgs args, yier.bubu.redis.execution.api.CommandSession session) {
        PreparedMutation<StringOps.SetStringValue> mutation = support.commandDb(session).strings()
                .prepareSet(args.key(), args.value(), args.mode(), args.expire(), args.getOld());
        StringOps.SetStringValue preview = mutation.preview();
        RedisReply reply = args.getOld()
                ? DbReplies.value(preview.oldValue())
                : preview.applied() ? RedisReplies.simpleString("OK") : RedisReplies.nullValue();
        return CommandSupport.preparedMutation(
                reply.shape(), mutation,
                execution -> {
                    mutation.commit();
                    return CommandResult.reply(reply);
                }
        );
    }

    private Function<CommandSession, PreparedCommand> get(CommandArgs args) {
        BytesSlice key = args.slice(1);
        return session -> {
            ByteValue value = support.commandDb(session).strings().getStringValue(key);
            return PreparedCommands.owned(CommandResult.reply(DbReplies.value(value)), value);
        };
    }

    private Function<CommandSession, PreparedCommand> strlen(CommandArgs args) {
        BytesSlice key = args.slice(1);
        return session -> PreparedCommands.ready(RedisReplies.integer(
                support.commandDb(session).strings().strlen(key)));
    }

    private Function<CommandSession, PreparedCommand> append(CommandArgs args) {
        byte[] key = args.bytes(1);
        BytesSlice value = args.slice(2);
        return session -> CommandSupport.preparedAction(ReplyShapes.integerUpperBound(), execution -> {
            long length = support.commandDb(execution).strings().append(key, value).value();
            return CommandResult.reply(RedisReplies.integer(length));
        });
    }

    private Function<CommandSession, PreparedCommand> setbit(CommandArgs args) {
        long offset = bitOffsetAt(args, 2);
        long value;
        try {
            value = args.longAt(3);
        } catch (CommandParseException failure) {
            throw new CommandParseException(INVALID_BIT);
        }
        if (value != 0L && value != 1L) {
            throw new CommandParseException(INVALID_BIT);
        }
        rejectBitOffsetBeyondMaxString(offset);
        SetBitArgs parsed = new SetBitArgs(args.bytes(1), offset, (int) value);
        return session -> CommandSupport.preparedAction(ReplyShapes.integerUpperBound(), execution -> {
            int previous = support.commandDb(execution).strings()
                    .setBit(parsed.key(), parsed.offset(), parsed.value()).value();
            return CommandResult.reply(RedisReplies.integer(previous));
        });
    }

    private Function<CommandSession, PreparedCommand> getbit(CommandArgs args) {
        long offset = bitOffsetAt(args, 2);
        rejectBitOffsetBeyondMaxString(offset);
        GetBitArgs parsed = new GetBitArgs(args.bytes(1), offset);
        BytesSlice key = args.slice(1);
        return session -> PreparedCommands.ready(RedisReplies.integer(
                support.commandDb(session).strings().getBit(key, parsed.offset())));
    }

    // Redis getBitOffsetFromArgument：非整数、负数 offset 共用同一条 "bit offset" 错误文案。
    private static long bitOffsetAt(CommandArgs args, int index) {
        long offset;
        try {
            offset = args.longAt(index);
        } catch (CommandParseException failure) {
            throw new CommandParseException(INVALID_BIT_OFFSET);
        }
        if (offset < 0L) {
            throw new CommandParseException(INVALID_BIT_OFFSET);
        }
        return offset;
    }

    // Redis getBitOffsetFromArgument 对越界 offset 复用 offset 文案，而不是 string 过大错误。
    // SETBIT 与 GETBIT 在解析期共用这条 512 MiB 上限。直接调用存储层 getBit 时，
    // 字节下标尚未超过 Integer.MAX_VALUE 的超长读偏移仍返回 0；setBit 则另以字符串长度错误拒绝。
    private static void rejectBitOffsetBeyondMaxString(long offset) {
        if (offset / 8L >= MAX_STRING_BYTES) {
            throw new CommandParseException(INVALID_BIT_OFFSET);
        }
    }

    // Redis bitcountCommand：arity 只要求 key，其余参数个数（只带 start、或多于 BIT|BYTE）在命令内报 syntax error，
    // 因此 MULTI 中这些错误会入队、EXEC 时才失败。start/end/unit 都在查 key 之前解析，参数错误优先于 WRONGTYPE。
    private Function<CommandSession, PreparedCommand> bitcount(CommandArgs args) {
        int argc = args.argc();
        BitCountArgs parsed;
        if (argc == 2) {
            parsed = new BitCountArgs(args.bytes(1), null, null, BitRangeUnit.BYTE);
        } else if (argc == 4 || argc == 5) {
            long start = args.longAt(2);
            long end = args.longAt(3);
            BitRangeUnit unit = argc == 5 ? bitRangeUnitAt(args, 4) : BitRangeUnit.BYTE;
            parsed = new BitCountArgs(args.bytes(1), start, end, unit);
        } else {
            throw syntaxFailure();
        }
        BytesSlice key = args.slice(1);
        return session -> {
            long count = parsed.start() == null
                    ? support.commandDb(session).strings().bitcount(key)
                    : support.commandDb(session).strings()
                            .bitcount(key, parsed.start(), parsed.end(), parsed.unit());
            return PreparedCommands.ready(RedisReplies.integer(count));
        };
    }

    private static BitRangeUnit bitRangeUnitAt(CommandArgs args, int index) {
        if (args.is(index, "BIT")) {
            return BitRangeUnit.BIT;
        }
        if (args.is(index, "BYTE")) {
            return BitRangeUnit.BYTE;
        }
        throw syntaxFailure();
    }

    private Function<CommandSession, PreparedCommand> incrBy(CommandArgs args, long delta) {
        byte[] key = args.bytes(1);
        return session -> CommandSupport.preparedAction(ReplyShapes.integerUpperBound(), execution -> {
            long value = support.commandDb(execution).strings().incrBy(key, delta).value();
            return CommandResult.reply(RedisReplies.integer(value));
        });
    }

    private static CommandParseException syntaxFailure() {
        return new CommandParseException(SYNTAX_ERROR);
    }
}
