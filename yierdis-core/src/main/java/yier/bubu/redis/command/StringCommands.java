package yier.bubu.redis.command;

import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.DbMemoryConstants;
import yier.bubu.redis.ops.StringOps;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespWriter;

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

    private void set(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 3) {
            CommandSupport.wrongArity(out, "set");
            return;
        }

        byte[] key = cmd.toByteArray(1);
        YierdisDb db = support.db(out);

        YierdisDb.SetMode mode = YierdisDb.SetMode.NORMAL;
        YierdisDb.ExpireOption expire = null;
        boolean getOld = false;

        for (int i = 3; i < cmd.argc(); i++) {
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "NX")) {
                if (mode == YierdisDb.SetMode.XX) {
                    out.error("ERR syntax error");
                    return;
                }
                if (mode == YierdisDb.SetMode.NX) {
                    out.error("ERR syntax error");
                    return;
                }
                mode = YierdisDb.SetMode.NX;
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "XX")) {
                if (mode == YierdisDb.SetMode.NX) {
                    out.error("ERR syntax error");
                    return;
                }
                if (mode == YierdisDb.SetMode.XX) {
                    out.error("ERR syntax error");
                    return;
                }
                mode = YierdisDb.SetMode.XX;
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
                expire = YierdisDb.ExpireOption.keepTtl();
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
                expire = YierdisDb.ExpireOption.ex(seconds);
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
                expire = YierdisDb.ExpireOption.px(millis);
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
                expire = YierdisDb.ExpireOption.exAt(unixSeconds);
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
                expire = YierdisDb.ExpireOption.pxAt(unixMillis);
                continue;
            }
            out.error("ERR syntax error");
            return;
        }

        boolean willSet = true;
        if (mode == YierdisDb.SetMode.NX) {
            willSet = !db.existsKey(support.argView(cmd, 1));
        } else if (mode == YierdisDb.SetMode.XX) {
            willSet = db.existsKey(support.argView(cmd, 1));
        }

        byte[] oldValueForGet = null;
        if (getOld && willSet) {
            byte[] old = db.getStringBytes(key);
            if (old != null) {
                oldValueForGet = Arrays.copyOf(old, old.length);
            }
        }

        if (willSet) {
            long extra = (long) Math.max(0, cmd.len(1)) + Math.max(0, cmd.len(2)) + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
            db.prepareWrite(extra);
        }

        StringOps strings = db.values().strings();
        boolean ok = strings.setString(key, cmd, 2, mode, expire);
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

    private void get(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "get");
            return;
        }
        YierdisDb db = support.db(out);
        db.values().strings().getStringForReply(support.argView(cmd, 1), support.bulkOut(out));
    }

    private void strlen(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "strlen");
            return;
        }
        YierdisDb db = support.db(out);
        out.integer(db.values().strings().strlen(support.argView(cmd, 1)));
    }

    private void append(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            CommandSupport.wrongArity(out, "append");
            return;
        }
        YierdisDb db = support.db(out);
        long extra = (long) Math.max(0, cmd.len(1)) + Math.max(0, cmd.len(2)) + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
        db.prepareWrite(extra);
        long len = db.values().strings().append(cmd.toByteArray(1), cmd, 2);
        out.integer(len);
    }

    private void setbit(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 4) {
            CommandSupport.wrongArity(out, "setbit");
            return;
        }
        YierdisDb db = support.db(out);
        long offset = CommandSupport.parseNonNegativeLong(cmd, 2, "offset");
        long v = CommandSupport.parseLong(cmd, 3, "value");
        if (v != 0 && v != 1) {
            out.error("ERR bit is not an integer or out of range");
            return;
        }

        int currentLen = db.strlen(support.argView(cmd, 1));
        long requiredBytes = (offset >>> 3) + 1;
        if (requiredBytes > MAX_STRING_BYTES) {
            out.error("ERR string exceeds maximum allowed size");
            return;
        }
        long growth = Math.max(0L, requiredBytes - (long) currentLen);
        long extra = (long) Math.max(0, cmd.len(1)) + growth + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
        db.prepareWrite(extra);

        int old = db.values().strings().setBit(cmd.toByteArray(1), offset, (int) v);
        out.integer(old);
    }

    private void getbit(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            CommandSupport.wrongArity(out, "getbit");
            return;
        }
        long offset = CommandSupport.parseNonNegativeLong(cmd, 2, "offset");
        YierdisDb db = support.db(out);
        out.integer(db.values().strings().getBit(support.argView(cmd, 1), offset));
    }

    private void bitcount(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2 && cmd.argc() != 4) {
            CommandSupport.wrongArity(out, "bitcount");
            return;
        }
        if (cmd.argc() == 2) {
            YierdisDb db = support.db(out);
            out.integer(db.values().strings().bitcount(support.argView(cmd, 1)));
            return;
        }
        long start = CommandSupport.parseLong(cmd, 2, "start");
        long end = CommandSupport.parseLong(cmd, 3, "end");
        YierdisDb db = support.db(out);
        out.integer(db.values().strings().bitcount(support.argView(cmd, 1), start, end));
    }

    private void incr(RespCommand cmd, RespWriter out) {
        incrBy(cmd, out, 1);
    }

    private void decr(RespCommand cmd, RespWriter out) {
        incrBy(cmd, out, -1);
    }

    private void incrBy(RespCommand cmd, RespWriter out, long delta) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, delta > 0 ? "incr" : "decr");
            return;
        }
        YierdisDb db = support.db(out);
        long extra = (long) Math.max(0, cmd.len(1)) + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
        db.prepareWrite(extra);
        long value = db.values().strings().incrBy(cmd.toByteArray(1), delta);
        out.integer(value);
    }
}
