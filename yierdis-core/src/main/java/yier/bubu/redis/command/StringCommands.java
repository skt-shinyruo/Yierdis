package yier.bubu.redis.command;

import yier.bubu.redis.db.DbMemoryConstants;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.ops.ExpireOption;
import yier.bubu.redis.ops.SetMode;
import yier.bubu.redis.ops.StringOps;
import yier.bubu.redis.protocol.Command;
import yier.bubu.redis.protocol.ReplyWriter;

import java.util.Arrays;
import java.util.Objects;

final class StringCommands {
    private static final long MAX_STRING_BYTES = 512L * 1024 * 1024;

    private final CommandSupport support;

    StringCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    void register(CommandRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        registry.register("SET", this::set);
        registry.register("GET", this::get);
        registry.register("STRLEN", this::strlen);
        registry.register("APPEND", this::append);
        registry.register("SETBIT", this::setbit);
        registry.register("GETBIT", this::getbit);
        registry.register("BITCOUNT", this::bitcount);
        registry.register("INCR", this::incr);
        registry.register("DECR", this::decr);
    }

    private void set(Command cmd, ReplyWriter out) {
        if (cmd.argc() < 3) {
            CommandSupport.wrongArity(out, "set");
            return;
        }

        byte[] key = cmd.toByteArray(1);
        DbEngine engine = support.db(out);

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
            willSet = !engine.keyspace().existsKey(support.argView(cmd, 1));
        } else if (mode == SetMode.XX) {
            willSet = engine.keyspace().existsKey(support.argView(cmd, 1));
        }

        byte[] oldValueForGet = null;
        if (getOld && willSet) {
            byte[] old = engine.values().strings().getStringBytes(key);
            if (old != null) {
                oldValueForGet = Arrays.copyOf(old, old.length);
            }
        }

        if (willSet) {
            long extra = (long) Math.max(0, cmd.len(1)) + Math.max(0, cmd.len(2)) + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
            engine.eviction().prepareWrite(extra);
        }

        StringOps strings = engine.values().strings();
        boolean ok = strings.setString(key, support.argSlice(cmd, 2), mode, expire);
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

    private void get(Command cmd, ReplyWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "get");
            return;
        }
        DbEngine engine = support.db(out);
        engine.values().strings().getStringForReply(support.argView(cmd, 1), out);
    }

    private void strlen(Command cmd, ReplyWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "strlen");
            return;
        }
        DbEngine engine = support.db(out);
        out.integer(engine.values().strings().strlen(support.argView(cmd, 1)));
    }

    private void append(Command cmd, ReplyWriter out) {
        if (cmd.argc() != 3) {
            CommandSupport.wrongArity(out, "append");
            return;
        }
        DbEngine engine = support.db(out);
        long extra = (long) Math.max(0, cmd.len(1)) + Math.max(0, cmd.len(2)) + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
        engine.eviction().prepareWrite(extra);
        long len = engine.values().strings().append(cmd.toByteArray(1), support.argSlice(cmd, 2));
        out.integer(len);
    }

    private void setbit(Command cmd, ReplyWriter out) {
        if (cmd.argc() != 4) {
            CommandSupport.wrongArity(out, "setbit");
            return;
        }
        DbEngine engine = support.db(out);
        long offset = CommandSupport.parseNonNegativeLong(cmd, 2, "offset");
        long v = CommandSupport.parseLong(cmd, 3, "value");
        if (v != 0 && v != 1) {
            out.error("ERR bit is not an integer or out of range");
            return;
        }

        long currentLen = engine.values().strings().strlen(support.argView(cmd, 1));
        long requiredBytes = (offset >>> 3) + 1;
        if (requiredBytes > MAX_STRING_BYTES) {
            out.error("ERR string exceeds maximum allowed size");
            return;
        }
        long growth = Math.max(0L, requiredBytes - currentLen);
        long extra = (long) Math.max(0, cmd.len(1)) + growth + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
        engine.eviction().prepareWrite(extra);

        int old = engine.values().strings().setBit(cmd.toByteArray(1), offset, (int) v);
        out.integer(old);
    }

    private void getbit(Command cmd, ReplyWriter out) {
        if (cmd.argc() != 3) {
            CommandSupport.wrongArity(out, "getbit");
            return;
        }
        long offset = CommandSupport.parseNonNegativeLong(cmd, 2, "offset");
        DbEngine engine = support.db(out);
        out.integer(engine.values().strings().getBit(support.argView(cmd, 1), offset));
    }

    private void bitcount(Command cmd, ReplyWriter out) {
        if (cmd.argc() != 2 && cmd.argc() != 4) {
            CommandSupport.wrongArity(out, "bitcount");
            return;
        }
        if (cmd.argc() == 2) {
            DbEngine engine = support.db(out);
            out.integer(engine.values().strings().bitcount(support.argView(cmd, 1)));
            return;
        }
        long start = CommandSupport.parseLong(cmd, 2, "start");
        long end = CommandSupport.parseLong(cmd, 3, "end");
        DbEngine engine = support.db(out);
        out.integer(engine.values().strings().bitcount(support.argView(cmd, 1), start, end));
    }

    private void incr(Command cmd, ReplyWriter out) {
        incrBy(cmd, out, 1);
    }

    private void decr(Command cmd, ReplyWriter out) {
        incrBy(cmd, out, -1);
    }

    private void incrBy(Command cmd, ReplyWriter out, long delta) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, delta > 0 ? "incr" : "decr");
            return;
        }
        DbEngine engine = support.db(out);
        long extra = (long) Math.max(0, cmd.len(1)) + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
        engine.eviction().prepareWrite(extra);
        long value = engine.values().strings().incrBy(cmd.toByteArray(1), delta);
        out.integer(value);
    }
}
