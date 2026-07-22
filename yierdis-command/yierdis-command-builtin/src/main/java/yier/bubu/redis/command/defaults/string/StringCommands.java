package yier.bubu.redis.command.defaults.string;

import yier.bubu.redis.command.api.ArgReader;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParseError;
import yier.bubu.redis.command.api.CommandParseResult;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.SlowCommandGovernor;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.defaults.BulkStringReplyAdapter;
import yier.bubu.redis.command.defaults.CommandSupport;

import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.StringWriteOps;
import yier.bubu.redis.storage.api.result.BulkStringValue;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReplyPlans;
import yier.bubu.redis.execution.api.RedisReplyWriter;

import java.util.Objects;

public final class StringCommands implements CommandModule {
    private static final long MAX_STRING_BYTES = 512L * 1024 * 1024;
    private static final String INVALID_SET_EXPIRE = "ERR invalid expire time in 'set' command";
    private static final CommandKeySpec KEY = new CommandKeySpec(1, 1, 1);

    private final CommandSupport support;

    public StringCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(CommandSpec.of(syntax("SET", CommandArity.min(3)), this::parseSet, this::set));
        registration.register(CommandSpec.of(syntax("GET", CommandArity.exact(2)), CommandParsers.args(), this::get));
        registration.register(CommandSpec.of(syntax("STRLEN", CommandArity.exact(2)), CommandParsers.args(), this::strlen));
        registration.register(CommandSpec.of(syntax("APPEND", CommandArity.exact(3)), CommandParsers.args(), this::append));
        registration.register(CommandSpec.of(syntax("SETBIT", CommandArity.exact(4)), CommandParsers.args(), this::setbit));
        registration.register(CommandSpec.of(syntax("GETBIT", CommandArity.exact(3)), CommandParsers.args(), this::getbit));
        registration.register(CommandSpec.of(syntax("BITCOUNT", CommandArity.oneOf(2, 4)), CommandParsers.request(), this::bitcount));
        registration.register(CommandSpec.of(syntax("INCR", CommandArity.exact(2)), CommandParsers.args(), this::incr));
        registration.register(CommandSpec.of(syntax("DECR", CommandArity.exact(2)), CommandParsers.args(), this::decr));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity) {
        return new CommandSyntax(nameUpper, arity, KEY, TransactionPolicy.QUEUEABLE);
    }

    private record SetArgs(ExecutionRequest request, byte[] key, int valueIndex, SetMode mode, ExpireOption expire, boolean getOld) {
    }

    private CommandParseResult<SetArgs> parseSet(ArgReader args) {
        byte[] key = args.bytes(1);
        SetMode mode = SetMode.NORMAL;
        ExpireOption expire = null;
        boolean getOld = false;

        for (int i = 3; i < args.argc(); i++) {
            if (args.is(i, "NX")) {
                if (mode != SetMode.NORMAL) {
                    return CommandParseResult.error(CommandParseError.syntax());
                }
                mode = SetMode.NX;
                continue;
            }
            if (args.is(i, "XX")) {
                if (mode != SetMode.NORMAL) {
                    return CommandParseResult.error(CommandParseError.syntax());
                }
                mode = SetMode.XX;
                continue;
            }
            if (args.is(i, "GET")) {
                if (getOld) {
                    return CommandParseResult.error(CommandParseError.syntax());
                }
                getOld = true;
                continue;
            }
            if (args.is(i, "KEEPTTL")) {
                if (expire != null) {
                    return CommandParseResult.error(CommandParseError.syntax());
                }
                expire = ExpireOption.keepTtl();
                continue;
            }
            if (args.is(i, "EX") || args.is(i, "PX") || args.is(i, "EXAT") || args.is(i, "PXAT")) {
                if (expire != null || i + 1 >= args.argc()) {
                    return CommandParseResult.error(CommandParseError.syntax());
                }
                String option = CommandSupport.utf8(args.bytes(i));
                long value;
                try {
                    value = args.longAt(++i);
                } catch (IllegalArgumentException e) {
                    return CommandParseResult.error(CommandParseError.integerOutOfRange());
                }
                if (value <= 0) {
                    return CommandParseResult.error(CommandParseError.custom(INVALID_SET_EXPIRE));
                }
                if ("EX".equalsIgnoreCase(option)) {
                    expire = ExpireOption.ex(value);
                    continue;
                }
                if ("PX".equalsIgnoreCase(option)) {
                    expire = ExpireOption.px(value);
                    continue;
                }
                if ("EXAT".equalsIgnoreCase(option)) {
                    long expireAtMillis;
                    try {
                        expireAtMillis = Math.multiplyExact(value, 1000L);
                    } catch (ArithmeticException e) {
                        expireAtMillis = Long.MAX_VALUE;
                    }
                    if (expireAtMillis <= System.currentTimeMillis()) {
                        return CommandParseResult.error(CommandParseError.custom(INVALID_SET_EXPIRE));
                    }
                    expire = ExpireOption.exAt(value);
                    continue;
                }
                if (value <= System.currentTimeMillis()) {
                    return CommandParseResult.error(CommandParseError.custom(INVALID_SET_EXPIRE));
                }
                expire = ExpireOption.pxAt(value);
                continue;
            }
            return CommandParseResult.error(CommandParseError.syntax());
        }
        return CommandParseResult.ok(new SetArgs(args.request(), key, 2, mode, expire, getOld));
    }

    private void set(SetArgs args, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (args.getOld()) {
            preflightSetGet(args, ctx, out);
        }
        StringWriteOps.SetStringValue result = support.commandDb(ctx).writes().strings().set(
                args.key(),
                support.argSlice(args.request(), args.valueIndex()),
                args.mode(),
                args.expire(),
                args.getOld()
        ).value();
        boolean ownershipTransferred = false;
        try {
            if (!result.applied()) {
                out.bulkString((byte[]) null);
                return;
            }
            if (args.getOld()) {
                CommandSupport.writeOwnedBulkString(out, result.oldValue());
                ownershipTransferred = true;
                return;
            }
            out.simpleString("OK");
        } finally {
            if (!ownershipTransferred) {
                result.close();
            }
        }
    }

    private void preflightSetGet(SetArgs args, CommandContext ctx, RedisReplyWriter out) {
        if (args.mode() == SetMode.NX) {
            out.requireReply(ReplyPlans.bulkString(-1, 0L));
            return;
        }
        try (BulkStringValue oldValue = support.commandDb(ctx).reads().strings()
                .previewStringValue(support.argView(args.request(), 1))) {
            out.requireReply(ReplyPlans.bulkString(oldValue.payloadLength(), oldValue.retainedMemoryBytes()));
        }
    }

    private void get(ArgReader args, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        ExecutionRequest request = args.request();
        BulkStringValue value = support.commandDb(ctx).reads().strings().getStringValue(support.argView(request, 1));
        CommandSupport.writeOwnedBulkString(out, value);
    }

    private void strlen(ArgReader args, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        ExecutionRequest request = args.request();
        out.integer(support.commandDb(ctx).reads().strings().strlen(support.argView(request, 1)));
    }

    private void append(ArgReader args, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        ExecutionRequest request = args.request();
        long length = support.commandDb(ctx).writes().strings()
                .append(request.readOnlyByteArray(1), support.argSlice(request, 2))
                .value();
        out.integer(length);
    }

    private void setbit(ArgReader args, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        ExecutionRequest request = args.request();
        long offset = CommandSupport.parseNonNegativeLong(request, 2, "offset");
        long v = CommandSupport.parseLong(request, 3, "value");
        if (v != 0 && v != 1) {
            out.error("ERR bit is not an integer or out of range");
            return;
        }

        long requiredBytes = (offset >>> 3) + 1;
        if (requiredBytes > MAX_STRING_BYTES) {
            out.error("ERR string exceeds maximum allowed size");
            return;
        }
        int previousBit = support.commandDb(ctx).writes().strings()
                .setBit(request.readOnlyByteArray(1), offset, (int) v)
                .value();
        out.integer(previousBit);
    }

    private void getbit(ArgReader args, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        ExecutionRequest request = args.request();
        long offset = CommandSupport.parseNonNegativeLong(request, 2, "offset");
        out.integer(support.commandDb(ctx).reads().strings().getBit(support.argView(request, 1), offset));
    }

    private void bitcount(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 2 && request.argc() != 4) {
            CommandSupport.wrongArity(out, "bitcount");
            return;
        }
        if (request.argc() == 2) {
            out.integer(support.commandDb(ctx).reads().strings().bitcount(support.argView(request, 1)));
            return;
        }
        long start = CommandSupport.parseLong(request, 2, "start");
        long end = CommandSupport.parseLong(request, 3, "end");
        out.integer(support.commandDb(ctx).reads().strings().bitcount(support.argView(request, 1), start, end));
    }

    private void incr(ArgReader args, CommandContext ctx) {
        incrBy(args, ctx, 1);
    }

    private void decr(ArgReader args, CommandContext ctx) {
        incrBy(args, ctx, -1);
    }

    private void incrBy(ArgReader args, CommandContext ctx, long delta) {
        RedisReplyWriter out = ctx.out();
        ExecutionRequest request = args.request();
        long value = support.commandDb(ctx).writes().strings()
                .incrBy(request.readOnlyByteArray(1), delta)
                .value();
        out.integer(value);
    }
}
