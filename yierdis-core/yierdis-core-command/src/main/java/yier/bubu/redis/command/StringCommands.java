package yier.bubu.redis.command;

import yier.bubu.redis.ops.ExpireOption;
import yier.bubu.redis.ops.SetMode;
import yier.bubu.redis.contract.Command;
import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ReplyWriter;

import java.util.Arrays;
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
        registration.register("SET", this::set);
        registration.register("GET", this::get);
        registration.register("STRLEN", this::strlen);
        registration.register("APPEND", this::append);
        registration.register("SETBIT", this::setbit);
        registration.register("GETBIT", this::getbit);
        registration.register("BITCOUNT", this::bitcount);
        registration.register("INCR", this::incr);
        registration.register("DECR", this::decr);
    }

    private void set(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() < 3) {
            CommandSupport.wrongArity(out, "set");
            return;
        }

        byte[] key = cmd.toByteArray(1);
        SetMode mode = SetMode.NORMAL;
        ExpireOption expire = null;
        boolean getOld = false;

        for (int i = 3; i < cmd.argc(); i++) {
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "NX")) {
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
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "XX")) {
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
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "GET")) {
                if (getOld) {
                    out.error("ERR syntax error");
                    return;
                }
                getOld = true;
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "KEEPTTL")) {
                if (expire != null) {
                    out.error("ERR syntax error");
                    return;
                }
                expire = ExpireOption.keepTtl();
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "EX")) {
                if (expire != null || i + 1 >= cmd.argc()) {
                    out.error("ERR syntax error");
                    return;
                }
                long seconds = CommandSupport.parseLong(cmd, ++i, "seconds");
                if (seconds <= 0) {
                    out.error("ERR invalid expire time in 'set' command");
                    return;
                }
                expire = ExpireOption.ex(seconds);
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "PX")) {
                if (expire != null || i + 1 >= cmd.argc()) {
                    out.error("ERR syntax error");
                    return;
                }
                long millis = CommandSupport.parseLong(cmd, ++i, "milliseconds");
                if (millis <= 0) {
                    out.error("ERR invalid expire time in 'set' command");
                    return;
                }
                expire = ExpireOption.px(millis);
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "EXAT")) {
                if (expire != null || i + 1 >= cmd.argc()) {
                    out.error("ERR syntax error");
                    return;
                }
                long unixSeconds = CommandSupport.parseLong(cmd, ++i, "seconds");
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
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "PXAT")) {
                if (expire != null || i + 1 >= cmd.argc()) {
                    out.error("ERR syntax error");
                    return;
                }
                long unixMillis = CommandSupport.parseLong(cmd, ++i, "milliseconds");
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

        boolean willSet = true;
        if (mode == SetMode.NX) {
            willSet = !support.dbReads(ctx).keyspace().existsKey(support.argView(cmd, 1));
        } else if (mode == SetMode.XX) {
            willSet = support.dbReads(ctx).keyspace().existsKey(support.argView(cmd, 1));
        }

        byte[] oldValueForGet = null;
        if (getOld && willSet) {
            byte[] old = support.dbReads(ctx).strings().getStringBytes(key);
            if (old != null) {
                oldValueForGet = Arrays.copyOf(old, old.length);
            }
        }

        boolean ok = support.dbWrites(ctx).strings().setString(key, support.argSlice(cmd, 2), mode, expire);
        if (!ok) {
            out.bulkString((byte[]) null);
            return;
        }
        if (getOld) {
            out.bulkString(oldValueForGet);
            return;
        }
        out.simpleString("OK");
    }

    private void get(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "get");
            return;
        }
        support.dbReads(ctx).strings().getStringValue(support.argView(cmd, 1)).writeTo(new BulkStringReplyAdapter(out));
    }

    private void strlen(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "strlen");
            return;
        }
        out.integer(support.dbReads(ctx).strings().strlen(support.argView(cmd, 1)));
    }

    private void append(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 3) {
            CommandSupport.wrongArity(out, "append");
            return;
        }
        long len = support.dbWrites(ctx).strings().append(cmd.toByteArray(1), support.argSlice(cmd, 2));
        out.integer(len);
    }

    private void setbit(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 4) {
            CommandSupport.wrongArity(out, "setbit");
            return;
        }
        long offset = CommandSupport.parseNonNegativeLong(cmd, 2, "offset");
        long v = CommandSupport.parseLong(cmd, 3, "value");
        if (v != 0 && v != 1) {
            out.error("ERR bit is not an integer or out of range");
            return;
        }

        long requiredBytes = (offset >>> 3) + 1;
        if (requiredBytes > MAX_STRING_BYTES) {
            out.error("ERR string exceeds maximum allowed size");
            return;
        }
        int old = support.dbWrites(ctx).strings().setBit(cmd.toByteArray(1), offset, (int) v);
        out.integer(old);
    }

    private void getbit(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 3) {
            CommandSupport.wrongArity(out, "getbit");
            return;
        }
        long offset = CommandSupport.parseNonNegativeLong(cmd, 2, "offset");
        out.integer(support.dbReads(ctx).strings().getBit(support.argView(cmd, 1), offset));
    }

    private void bitcount(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 2 && cmd.argc() != 4) {
            CommandSupport.wrongArity(out, "bitcount");
            return;
        }
        if (cmd.argc() == 2) {
            out.integer(support.dbReads(ctx).strings().bitcount(support.argView(cmd, 1)));
            return;
        }
        long start = CommandSupport.parseLong(cmd, 2, "start");
        long end = CommandSupport.parseLong(cmd, 3, "end");
        out.integer(support.dbReads(ctx).strings().bitcount(support.argView(cmd, 1), start, end));
    }

    private void incr(Command cmd, CommandContext ctx) {
        incrBy(cmd, ctx, 1);
    }

    private void decr(Command cmd, CommandContext ctx) {
        incrBy(cmd, ctx, -1);
    }

    private void incrBy(Command cmd, CommandContext ctx, long delta) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, delta > 0 ? "incr" : "decr");
            return;
        }
        long value = support.dbWrites(ctx).strings().incrBy(cmd.toByteArray(1), delta);
        out.integer(value);
    }
}
