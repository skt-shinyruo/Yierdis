package yier.bubu.redis.command;

import yier.bubu.redis.ops.ExpireOption;
import yier.bubu.redis.ops.SetMode;
import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriter;

import java.util.Objects;

final class StringCommands implements CommandModule {
    private static final long MAX_STRING_BYTES = 512L * 1024 * 1024;

    private final CommandSupport support;

    StringCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register("SET", this::set, CommandDescriptor.of(-3, 1, 1, 1));
        registration.register("GET", CommandDescriptor.of(2, 1, 1, 1), CommandParsers.exact(2, "get"), this::get);
        registration.register("STRLEN", CommandDescriptor.of(2, 1, 1, 1), CommandParsers.exact(2, "strlen"), this::strlen);
        registration.register("APPEND", CommandDescriptor.of(3, 1, 1, 1), CommandParsers.exact(3, "append"), this::append);
        registration.register("SETBIT", CommandDescriptor.of(4, 1, 1, 1), CommandParsers.exact(4, "setbit"), this::setbit);
        registration.register("GETBIT", CommandDescriptor.of(3, 1, 1, 1), CommandParsers.exact(3, "getbit"), this::getbit);
        registration.register("BITCOUNT", this::bitcount, CommandDescriptor.of(-2, 1, 1, 1));
        registration.register("INCR", CommandDescriptor.of(2, 1, 1, 1), CommandParsers.exact(2, "incr"), this::incr);
        registration.register("DECR", CommandDescriptor.of(2, 1, 1, 1), CommandParsers.exact(2, "decr"), this::decr);
    }

    private void set(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() < 3) {
            CommandSupport.wrongArity(out, "set");
            return;
        }

        byte[] key = request.readOnlyByteArray(1);
        SetMode mode = SetMode.NORMAL;
        ExpireOption expire = null;
        boolean getOld = false;

        for (int i = 3; i < request.argc(); i++) {
            if (CommandSupport.asciiEqualsIgnoreCase(request, i, "NX")) {
                if (mode == SetMode.XX) {
                    out.error("ERR syntax error");
                    return;
                }
                if (mode == SetMode.NX) {
                    out.error("ERR syntax error");
                    return;
                }
                mode = SetMode.NX;
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(request, i, "XX")) {
                if (mode == SetMode.NX) {
                    out.error("ERR syntax error");
                    return;
                }
                if (mode == SetMode.XX) {
                    out.error("ERR syntax error");
                    return;
                }
                mode = SetMode.XX;
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(request, i, "GET")) {
                if (getOld) {
                    out.error("ERR syntax error");
                    return;
                }
                getOld = true;
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(request, i, "KEEPTTL")) {
                if (expire != null) {
                    out.error("ERR syntax error");
                    return;
                }
                expire = ExpireOption.keepTtl();
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(request, i, "EX")) {
                if (expire != null || i + 1 >= request.argc()) {
                    out.error("ERR syntax error");
                    return;
                }
                long seconds = CommandSupport.parseLong(request, ++i, "seconds");
                if (seconds <= 0) {
                    out.error("ERR invalid expire time in 'set' command");
                    return;
                }
                expire = ExpireOption.ex(seconds);
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(request, i, "PX")) {
                if (expire != null || i + 1 >= request.argc()) {
                    out.error("ERR syntax error");
                    return;
                }
                long millis = CommandSupport.parseLong(request, ++i, "milliseconds");
                if (millis <= 0) {
                    out.error("ERR invalid expire time in 'set' command");
                    return;
                }
                expire = ExpireOption.px(millis);
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(request, i, "EXAT")) {
                if (expire != null || i + 1 >= request.argc()) {
                    out.error("ERR syntax error");
                    return;
                }
                long unixSeconds = CommandSupport.parseLong(request, ++i, "seconds");
                if (unixSeconds <= 0) {
                    out.error("ERR invalid expire time in 'set' command");
                    return;
                }
                long expireAtMillis;
                try {
                    expireAtMillis = Math.multiplyExact(unixSeconds, 1000L);
                } catch (ArithmeticException e) {
                    expireAtMillis = Long.MAX_VALUE;
                }
                if (expireAtMillis <= System.currentTimeMillis()) {
                    out.error("ERR invalid expire time in 'set' command");
                    return;
                }
                expire = ExpireOption.exAt(unixSeconds);
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(request, i, "PXAT")) {
                if (expire != null || i + 1 >= request.argc()) {
                    out.error("ERR syntax error");
                    return;
                }
                long unixMillis = CommandSupport.parseLong(request, ++i, "milliseconds");
                if (unixMillis <= 0) {
                    out.error("ERR invalid expire time in 'set' command");
                    return;
                }
                if (unixMillis <= System.currentTimeMillis()) {
                    out.error("ERR invalid expire time in 'set' command");
                    return;
                }
                expire = ExpireOption.pxAt(unixMillis);
                continue;
            }
            out.error("ERR syntax error");
            return;
        }

        var result = support.dbWrites(ctx).strings().set(key, support.argSlice(request, 2), mode, expire, getOld);
        if (!result.applied()) {
            out.bulkString((byte[]) null);
            return;
        }
        if (getOld) {
            out.bulkString(result.oldValue());
            return;
        }
        out.simpleString("OK");
    }

    private void get(ArgReader args, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        ExecutionRequest request = args.request();
        support.dbReads(ctx).strings().getStringValue(support.argView(request, 1)).writeTo(new BulkStringReplyAdapter(out));
    }

    private void strlen(ArgReader args, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        ExecutionRequest request = args.request();
        out.integer(support.dbReads(ctx).strings().strlen(support.argView(request, 1)));
    }

    private void append(ArgReader args, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        ExecutionRequest request = args.request();
        long len = support.dbWrites(ctx).strings().append(request.readOnlyByteArray(1), support.argSlice(request, 2));
        out.integer(len);
    }

    private void setbit(ArgReader args, CommandContext ctx) {
        ReplyWriter out = ctx.out();
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
        int old = support.dbWrites(ctx).strings().setBit(request.readOnlyByteArray(1), offset, (int) v);
        out.integer(old);
    }

    private void getbit(ArgReader args, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        ExecutionRequest request = args.request();
        long offset = CommandSupport.parseNonNegativeLong(request, 2, "offset");
        out.integer(support.dbReads(ctx).strings().getBit(support.argView(request, 1), offset));
    }

    private void bitcount(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() != 2 && request.argc() != 4) {
            CommandSupport.wrongArity(out, "bitcount");
            return;
        }
        if (request.argc() == 2) {
            out.integer(support.dbReads(ctx).strings().bitcount(support.argView(request, 1)));
            return;
        }
        long start = CommandSupport.parseLong(request, 2, "start");
        long end = CommandSupport.parseLong(request, 3, "end");
        out.integer(support.dbReads(ctx).strings().bitcount(support.argView(request, 1), start, end));
    }

    private void incr(ArgReader args, CommandContext ctx) {
        incrBy(args, ctx, 1);
    }

    private void decr(ArgReader args, CommandContext ctx) {
        incrBy(args, ctx, -1);
    }

    private void incrBy(ArgReader args, CommandContext ctx, long delta) {
        ReplyWriter out = ctx.out();
        ExecutionRequest request = args.request();
        long value = support.dbWrites(ctx).strings().incrBy(request.readOnlyByteArray(1), delta);
        out.integer(value);
    }
}
