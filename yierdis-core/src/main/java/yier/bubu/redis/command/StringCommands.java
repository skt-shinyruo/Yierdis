package yier.bubu.redis.command;

import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespWriter;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

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

        YierdisDb.SetMode mode = YierdisDb.SetMode.NORMAL;
        YierdisDb.ExpireOption expire = null;

        for (int i = 3; i < cmd.argc(); i++) {
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "NX")) {
                mode = YierdisDb.SetMode.NX;
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "XX")) {
                mode = YierdisDb.SetMode.XX;
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "EX") && i + 1 < cmd.argc()) {
                long seconds = CommandSupport.parseLong(cmd, ++i, "seconds");
                expire = new YierdisDb.ExpireOption(TimeUnit.SECONDS, seconds);
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "PX") && i + 1 < cmd.argc()) {
                long millis = CommandSupport.parseLong(cmd, ++i, "milliseconds");
                expire = new YierdisDb.ExpireOption(TimeUnit.MILLISECONDS, millis);
                continue;
            }
            out.error("ERR syntax error");
            return;
        }

        boolean willSet = true;
        if (mode == YierdisDb.SetMode.NX) {
            willSet = !support.db().existsKey(support.argView(cmd, 1));
        } else if (mode == YierdisDb.SetMode.XX) {
            willSet = support.db().existsKey(support.argView(cmd, 1));
        }
        if (willSet) {
            long extra = (long) Math.max(0, cmd.len(1)) + Math.max(0, cmd.len(2)) + 16L;
            support.db().ensureWriteAllowed(extra);
        }

        boolean ok = support.db().setString(key, cmd, 2, mode, expire);
        if (!ok) {
            out.bulkString((byte[]) null);
            return;
        }
        support.db().enforceMaxmemory();
        out.simpleString("OK");
    }

    private void get(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "get");
            return;
        }
        support.db().getStringForReply(support.argView(cmd, 1), support.bulkOut(out));
    }

    private void strlen(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "strlen");
            return;
        }
        out.integer(support.db().strlen(support.argView(cmd, 1)));
    }

    private void append(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            CommandSupport.wrongArity(out, "append");
            return;
        }
        long extra = (long) Math.max(0, cmd.len(1)) + Math.max(0, cmd.len(2)) + 16L;
        support.db().ensureWriteAllowed(extra);
        out.integer(support.db().append(cmd.toByteArray(1), cmd, 2));
        support.db().enforceMaxmemory();
    }

    private void setbit(RespCommand cmd, RespWriter out) {
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

        int currentLen = support.db().strlen(support.argView(cmd, 1));
        long requiredBytes = (offset >>> 3) + 1;
        if (requiredBytes > MAX_STRING_BYTES) {
            out.error("ERR string exceeds maximum allowed size");
            return;
        }
        long growth = Math.max(0L, requiredBytes - (long) currentLen);
        long extra = (long) Math.max(0, cmd.len(1)) + growth + 16L;
        support.db().ensureWriteAllowed(extra);

        int old = support.db().setBit(cmd.toByteArray(1), offset, (int) v);
        support.db().enforceMaxmemory();
        out.integer(old);
    }

    private void getbit(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            CommandSupport.wrongArity(out, "getbit");
            return;
        }
        long offset = CommandSupport.parseNonNegativeLong(cmd, 2, "offset");
        out.integer(support.db().getBit(support.argView(cmd, 1), offset));
    }

    private void bitcount(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2 && cmd.argc() != 4) {
            CommandSupport.wrongArity(out, "bitcount");
            return;
        }
        if (cmd.argc() == 2) {
            out.integer(support.db().bitcount(support.argView(cmd, 1)));
            return;
        }
        long start = CommandSupport.parseLong(cmd, 2, "start");
        long end = CommandSupport.parseLong(cmd, 3, "end");
        out.integer(support.db().bitcount(support.argView(cmd, 1), start, end));
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
        long extra = (long) Math.max(0, cmd.len(1)) + 16L;
        support.db().ensureWriteAllowed(extra);
        out.integer(support.db().incrBy(cmd.toByteArray(1), delta));
        support.db().enforceMaxmemory();
    }
}

