package yier.bubu.redis.command;

import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.YierdisBulkStringOutput;
import yier.bubu.redis.db.YierdisBytesView;
import yier.bubu.redis.db.ValueType;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapOutOfMemoryException;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapSlice;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespProtocol;
import yier.bubu.redis.protocol.RespWriter;

import java.nio.charset.StandardCharsets;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.concurrent.TimeUnit;

/**
 * A server-side command processor optimized for low allocation.
 * <p>
 * It executes commands and writes RESP2 replies directly via {@link RespWriter}.
 */
public final class YierdisFastCommandProcessor {
    private static final String NULL_BULK_STRING_ERR = "ERR Protocol error: null bulk string";
    private static final int HLL_DENSE_BYTES = 8 + 12288;
    private static final long MAX_STRING_BYTES = 512L * 1024 * 1024;

    private static final byte[] HELLO_SERVER_KEY = "server".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_SERVER_VALUE = "yierdis".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_VERSION_KEY = "version".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_VERSION_VALUE = "0.1.0".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_PROTO_KEY = "proto".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_PROTO_VALUE = "2".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_PROTO_VALUE_RESP3 = "3".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_MODE_KEY = "mode".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_MODE_VALUE = "standalone".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_ROLE_KEY = "role".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_ROLE_VALUE = "master".getBytes(StandardCharsets.US_ASCII);

    private final YierdisDb db;
    private final ByteArraySliceList slice = new ByteArraySliceList();
    private byte[][] argvScratch = new byte[16][];
    private final RespCommandArgBytesView argView = new RespCommandArgBytesView();
    private final WriterBulkStringOutput bulkOut = new WriterBulkStringOutput();
    private final Command[] commandTable;

    public YierdisFastCommandProcessor(YierdisDb db) {
        this.db = db;
        this.commandTable = new Command[]{
                new Command("PING", this::ping),
                new Command("ECHO", this::echo),
                new Command("HELLO", this::hello),
                new Command("COMMAND", (cmd, out) -> out.emptyArray()),
                new Command("SELECT", this::select),
                new Command("FLUSHDB", (cmd, out) -> {
                    db.flushDb();
                    out.simpleString("OK");
                }),
                new Command("TYPE", this::type),
                new Command("MEMORY", this::memory),
                new Command("OBJECT", this::object),
                new Command("KEYS", this::keys),
                new Command("DEL", this::del),
                new Command("EXISTS", this::exists),
                new Command("SET", this::set),
                new Command("GET", this::get),
                new Command("STRLEN", this::strlen),
                new Command("APPEND", this::append),
                new Command("SETBIT", this::setbit),
                new Command("GETBIT", this::getbit),
                new Command("BITCOUNT", this::bitcount),
                new Command("PFADD", this::pfadd),
                new Command("PFCOUNT", this::pfcount),
                new Command("PFMERGE", this::pfmerge),
                new Command("INCR", this::incr),
                new Command("DECR", this::decr),
                new Command("EXPIRE", this::expire),
                new Command("TTL", this::ttl),
                new Command("LPUSH", this::lpush),
                new Command("RPUSH", this::rpush),
                new Command("LRANGE", this::lrange),
                new Command("LPOP", this::lpop),
                new Command("RPOP", this::rpop),
                new Command("HSET", this::hset),
                new Command("HGET", this::hget),
                new Command("HGETALL", this::hgetall),
                new Command("HLEN", this::hlen),
                new Command("HDEL", this::hdel),
                new Command("SADD", this::sadd),
                new Command("SREM", this::srem),
                new Command("SMEMBERS", this::smembers),
                new Command("SISMEMBER", this::sismember),
                new Command("SCARD", this::scard),
                new Command("ZADD", this::zadd),
                new Command("ZRANGE", this::zrange),
                new Command("ZREVRANGE", this::zrevrange),
                new Command("ZRANGEBYSCORE", this::zrangebyscore),
                new Command("ZREVRANGEBYSCORE", this::zrevrangebyscore),
                new Command("ZREMRANGEBYSCORE", this::zremrangebyscore),
                new Command("ZREMRANGEBYRANK", this::zremrangebyrank),
                new Command("ZREM", this::zrem),
        };
    }

    public void execute(RespCommand cmd, RespWriter out) {
        int argc = cmd.argc();
        if (argc <= 0) {
            out.error("ERR empty command");
            return;
        }
        if (cmd.isNull(0) || cmd.len(0) == 0) {
            out.error("ERR empty command");
            return;
        }

        // Reject null bulk strings early to avoid NPEs deeper in the DB and data structures.
        // We only allow a null bulk string for PING/ECHO's single message argument (argv[1]).
        boolean allowNullMessage = asciiEqualsIgnoreCase(cmd, 0, "PING") || asciiEqualsIgnoreCase(cmd, 0, "ECHO");
        for (int i = 1; i < argc; i++) {
            if (!cmd.isNull(i)) {
                continue;
            }
            if (allowNullMessage && argc == 2 && i == 1) {
                continue;
            }
            out.error(NULL_BULK_STRING_ERR);
            return;
        }

        try {
            for (Command c : commandTable) {
                if (asciiEqualsIgnoreCase(cmd, 0, c.name)) {
                    c.handler.execute(cmd, out);
                    return;
                }
            }
            out.error("ERR unknown command");
        } catch (YierdisDb.WrongTypeException e) {
            out.error(e.getMessage());
        } catch (YierdisDb.YierdisCommandException e) {
            out.error(e.getMessage());
        } catch (YierdisOffHeapOutOfMemoryException e) {
            out.error("OOM off-heap memory limit exceeded");
        } catch (IllegalArgumentException e) {
            out.error("ERR " + e.getMessage());
        }
    }

    private void incr(RespCommand cmd, RespWriter out) {
        incrBy(cmd, out, 1);
    }

    private void decr(RespCommand cmd, RespWriter out) {
        incrBy(cmd, out, -1);
    }

    private void lpush(RespCommand cmd, RespWriter out) {
        push(cmd, out, true);
    }

    private void rpush(RespCommand cmd, RespWriter out) {
        push(cmd, out, false);
    }

    private void lpop(RespCommand cmd, RespWriter out) {
        pop(cmd, out, true);
    }

    private void rpop(RespCommand cmd, RespWriter out) {
        pop(cmd, out, false);
    }

    @FunctionalInterface
    private interface CommandHandler {
        void execute(RespCommand cmd, RespWriter out);
    }

    private static final class Command {
        private final String name;
        private final CommandHandler handler;

        private Command(String name, CommandHandler handler) {
            this.name = Objects.requireNonNull(name, "name");
            this.handler = Objects.requireNonNull(handler, "handler");
        }
    }

    private void ping(RespCommand cmd, RespWriter out) {
        if (cmd.argc() == 1) {
            out.simpleString("PONG");
            return;
        }
        if (cmd.argc() == 2) {
            out.bulkString(cmd.toByteArray(1));
            return;
        }
        wrongArity(out, "ping");
    }

    private void echo(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            wrongArity(out, "echo");
            return;
        }
        out.bulkString(cmd.toByteArray(1));
    }

    private void hello(RespCommand cmd, RespWriter out) {
        // Minimal HELLO implementation (RESP2 + RESP3 handshake).
        String version = cmd.argc() >= 2 ? utf8(cmd, 1) : "2";
        if ("3".equals(version)) {
            // Switch the connection to RESP3 and return a map as required by RESP3 clients.
            out.setProtocol(RespProtocol.RESP3);
            out.mapHeader(5);
            out.bulkString(HELLO_SERVER_KEY);
            out.bulkString(HELLO_SERVER_VALUE);
            out.bulkString(HELLO_VERSION_KEY);
            out.bulkString(HELLO_VERSION_VALUE);
            out.bulkString(HELLO_PROTO_KEY);
            out.bulkString(HELLO_PROTO_VALUE_RESP3);
            out.bulkString(HELLO_MODE_KEY);
            out.bulkString(HELLO_MODE_VALUE);
            out.bulkString(HELLO_ROLE_KEY);
            out.bulkString(HELLO_ROLE_VALUE);
            return;
        }
        if (!"2".equals(version)) {
            out.error("ERR unsupported protocol version");
            return;
        }

        // Switch back to RESP2 when explicitly requested.
        out.setProtocol(RespProtocol.RESP2);
        out.arrayHeader(10);
        out.bulkString(HELLO_SERVER_KEY);
        out.bulkString(HELLO_SERVER_VALUE);
        out.bulkString(HELLO_VERSION_KEY);
        out.bulkString(HELLO_VERSION_VALUE);
        out.bulkString(HELLO_PROTO_KEY);
        out.bulkString(HELLO_PROTO_VALUE);
        out.bulkString(HELLO_MODE_KEY);
        out.bulkString(HELLO_MODE_VALUE);
        out.bulkString(HELLO_ROLE_KEY);
        out.bulkString(HELLO_ROLE_VALUE);
    }

    private void select(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            wrongArity(out, "select");
            return;
        }
        if (!cmd.isNull(1) && cmd.len(1) == 1 && cmd.byteAt(1, 0) == '0') {
            out.simpleString("OK");
            return;
        }
        out.error("ERR only DB 0 is supported");
    }

    private void type(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            wrongArity(out, "type");
            return;
        }
        ValueType t = db.typeOf(argView.reset(cmd, 1));
        if (t == null) {
            out.simpleString("none");
            return;
        }
        out.simpleString(t.name().toLowerCase(Locale.ROOT));
    }

    private void memory(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            wrongArity(out, "memory");
            return;
        }
        if (!asciiEqualsIgnoreCase(cmd, 1, "USAGE")) {
            out.error("ERR syntax error");
            return;
        }
        long bytes = db.memoryUsage(argView.reset(cmd, 2));
        if (bytes < 0) {
            out.bulkString((byte[]) null);
            return;
        }
        out.integer(bytes);
    }

    private void object(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            wrongArity(out, "object");
            return;
        }
        if (!asciiEqualsIgnoreCase(cmd, 1, "ENCODING")) {
            out.error("ERR syntax error");
            return;
        }
        String enc = db.objectEncoding(argView.reset(cmd, 2));
        if (enc == null) {
            out.bulkString((byte[]) null);
            return;
        }
        out.simpleString(enc);
    }

    private void keys(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            wrongArity(out, "keys");
            return;
        }
        out.bulkStringArray(db.keys(cmd.toByteArray(1)));
    }

    private void del(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 2) {
            wrongArity(out, "del");
            return;
        }
        int len = cmd.argc() - 1;
        sliceResetFromCommand(cmd, 1, len);
        try {
            out.integer(db.del(slice));
        } finally {
            clearScratch(len);
        }
    }

    private void exists(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 2) {
            wrongArity(out, "exists");
            return;
        }

        long count = 0;
        for (int i = 1; i < cmd.argc(); i++) {
            if (db.existsKey(argView.reset(cmd, i))) {
                count++;
            }
        }
        out.integer(count);
    }

    private void set(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 3) {
            wrongArity(out, "set");
            return;
        }

        byte[] key = cmd.toByteArray(1);

        YierdisDb.SetMode mode = YierdisDb.SetMode.NORMAL;
        YierdisDb.ExpireOption expire = null;

        for (int i = 3; i < cmd.argc(); i++) {
            if (asciiEqualsIgnoreCase(cmd, i, "NX")) {
                mode = YierdisDb.SetMode.NX;
                continue;
            }
            if (asciiEqualsIgnoreCase(cmd, i, "XX")) {
                mode = YierdisDb.SetMode.XX;
                continue;
            }
            if (asciiEqualsIgnoreCase(cmd, i, "EX") && i + 1 < cmd.argc()) {
                long seconds = parseLong(cmd, ++i, "seconds");
                expire = new YierdisDb.ExpireOption(TimeUnit.SECONDS, seconds);
                continue;
            }
            if (asciiEqualsIgnoreCase(cmd, i, "PX") && i + 1 < cmd.argc()) {
                long millis = parseLong(cmd, ++i, "milliseconds");
                expire = new YierdisDb.ExpireOption(TimeUnit.MILLISECONDS, millis);
                continue;
            }
            out.error("ERR syntax error");
            return;
        }

        boolean willSet = true;
        if (mode == YierdisDb.SetMode.NX) {
            willSet = !db.existsKey(argView.reset(cmd, 1));
        } else if (mode == YierdisDb.SetMode.XX) {
            willSet = db.existsKey(argView.reset(cmd, 1));
        }
        if (willSet) {
            long extra = (long) Math.max(0, cmd.len(1)) + Math.max(0, cmd.len(2)) + 16L;
            db.ensureWriteAllowed(extra);
        }

        boolean ok = db.setString(key, cmd, 2, mode, expire);
        if (!ok) {
            out.bulkString((byte[]) null);
            return;
        }
        db.enforceMaxmemory();
        out.simpleString("OK");
    }

    private void get(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            wrongArity(out, "get");
            return;
        }
        bulkOut.reset(out);
        db.getStringForReply(argView.reset(cmd, 1), bulkOut);
    }

    private void strlen(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            wrongArity(out, "strlen");
            return;
        }
        out.integer(db.strlen(argView.reset(cmd, 1)));
    }

    private void append(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            wrongArity(out, "append");
            return;
        }
        long extra = (long) Math.max(0, cmd.len(1)) + Math.max(0, cmd.len(2)) + 16L;
        db.ensureWriteAllowed(extra);
        out.integer(db.append(cmd.toByteArray(1), cmd, 2));
        db.enforceMaxmemory();
    }

    private void setbit(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 4) {
            wrongArity(out, "setbit");
            return;
        }
        long offset = parseNonNegativeLong(cmd, 2, "offset");
        long v = parseLong(cmd, 3, "value");
        if (v != 0 && v != 1) {
            out.error("ERR bit is not an integer or out of range");
            return;
        }

        int currentLen = db.strlen(argView.reset(cmd, 1));
        long requiredBytes = (offset >>> 3) + 1;
        if (requiredBytes > MAX_STRING_BYTES) {
            out.error("ERR string exceeds maximum allowed size");
            return;
        }
        long growth = Math.max(0L, requiredBytes - (long) currentLen);
        long extra = (long) Math.max(0, cmd.len(1)) + growth + 16L;
        db.ensureWriteAllowed(extra);

        int old = db.setBit(cmd.toByteArray(1), offset, (int) v);
        db.enforceMaxmemory();
        out.integer(old);
    }

    private void getbit(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            wrongArity(out, "getbit");
            return;
        }
        long offset = parseNonNegativeLong(cmd, 2, "offset");
        out.integer(db.getBit(argView.reset(cmd, 1), offset));
    }

    private void bitcount(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2 && cmd.argc() != 4) {
            wrongArity(out, "bitcount");
            return;
        }
        if (cmd.argc() == 2) {
            out.integer(db.bitcount(argView.reset(cmd, 1)));
            return;
        }
        long start = parseLong(cmd, 2, "start");
        long end = parseLong(cmd, 3, "end");
        out.integer(db.bitcount(argView.reset(cmd, 1), start, end));
    }

    private void pfadd(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 3) {
            wrongArity(out, "pfadd");
            return;
        }
        long extra = (long) Math.max(0, cmd.len(1)) + HLL_DENSE_BYTES + 16L;
        db.ensureWriteAllowed(extra);
        out.integer(db.pfadd(cmd.toByteArray(1), cmd, 2));
        db.enforceMaxmemory();
    }

    private void pfcount(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 2) {
            wrongArity(out, "pfcount");
            return;
        }
        int len = cmd.argc() - 1;
        sliceResetFromCommand(cmd, 1, len);
        try {
            out.integer(db.pfcount(slice));
        } finally {
            clearScratch(len);
        }
    }

    private void pfmerge(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 3) {
            wrongArity(out, "pfmerge");
            return;
        }
        long extra = (long) Math.max(0, cmd.len(1)) + HLL_DENSE_BYTES + 16L;
        db.ensureWriteAllowed(extra);

        int sourcesLen = cmd.argc() - 2;
        sliceResetFromCommand(cmd, 2, sourcesLen);
        try {
            db.pfmerge(cmd.toByteArray(1), slice);
        } finally {
            clearScratch(sourcesLen);
        }
        db.enforceMaxmemory();
        out.simpleString("OK");
    }

    private void incrBy(RespCommand cmd, RespWriter out, long delta) {
        if (cmd.argc() != 2) {
            wrongArity(out, delta > 0 ? "incr" : "decr");
            return;
        }
        long extra = (long) Math.max(0, cmd.len(1)) + 16L;
        db.ensureWriteAllowed(extra);
        out.integer(db.incrBy(cmd.toByteArray(1), delta));
        db.enforceMaxmemory();
    }

    private void expire(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            wrongArity(out, "expire");
            return;
        }
        long seconds = parseLong(cmd, 2, "seconds");
        out.integer(db.expire(argView.reset(cmd, 1), seconds) ? 1 : 0);
    }

    private void ttl(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            wrongArity(out, "ttl");
            return;
        }
        out.integer(db.ttlSeconds(argView.reset(cmd, 1)));
    }

    private void push(RespCommand cmd, RespWriter out, boolean left) {
        if (cmd.argc() < 3) {
            wrongArity(out, left ? "lpush" : "rpush");
            return;
        }
        long extra = (long) Math.max(0, cmd.len(1)) + 16L;
        for (int i = 2; i < cmd.argc(); i++) {
            extra += Math.max(0, cmd.len(i));
        }
        db.ensureWriteAllowed(extra);
        int valuesLen = cmd.argc() - 2;
        sliceResetFromCommand(cmd, 2, valuesLen);
        try {
            int len = left
                    ? db.lpush(cmd.toByteArray(1), slice)
                    : db.rpush(cmd.toByteArray(1), slice);
            db.enforceMaxmemory();
            out.integer(len);
        } finally {
            clearScratch(valuesLen);
        }
    }

    private void lrange(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 4) {
            wrongArity(out, "lrange");
            return;
        }
        int start = parseIntClamped(cmd, 2, "start");
        int stop = parseIntClamped(cmd, 3, "stop");

        byte[] key = cmd.toByteArray(1);
        int count = db.lrangeReplyCount(key, start, stop);
        out.arrayHeader(count);
        if (count == 0) {
            return;
        }
        bulkOut.reset(out);
        db.lrangeReplyInto(key, start, stop, bulkOut);
    }

    private void pop(RespCommand cmd, RespWriter out, boolean left) {
        if (cmd.argc() != 2 && cmd.argc() != 3) {
            wrongArity(out, left ? "lpop" : "rpop");
            return;
        }
        int count = 1;
        boolean hasCount = cmd.argc() == 3;
        if (hasCount) {
            count = parseIntClamped(cmd, 2, "count");
        }

        List<byte[]> popped = left
                ? db.lpop(cmd.toByteArray(1), count)
                : db.rpop(cmd.toByteArray(1), count);
        popResponse(out, popped, hasCount);
    }

    private static void popResponse(RespWriter out, List<byte[]> popped, boolean hasCount) {
        if (!hasCount) {
            if (popped.isEmpty()) {
                out.bulkString((byte[]) null);
                return;
            }
            out.bulkString(popped.get(0));
            return;
        }
        out.bulkStringArray(popped);
    }

    private void hset(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 4) {
            wrongArity(out, "hset");
            return;
        }
        long extra = (long) Math.max(0, cmd.len(1)) + 16L;
        for (int i = 2; i < cmd.argc(); i++) {
            extra += Math.max(0, cmd.len(i));
        }
        db.ensureWriteAllowed(extra);
        int pairsLen = cmd.argc() - 2;
        sliceResetFromCommand(cmd, 2, pairsLen);
        try {
            out.integer(db.hset(cmd.toByteArray(1), slice));
            db.enforceMaxmemory();
        } finally {
            clearScratch(pairsLen);
        }
    }

    private void hget(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            wrongArity(out, "hget");
            return;
        }
        out.bulkString(db.hget(cmd.toByteArray(1), cmd.toByteArray(2)));
    }

    private void hgetall(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            wrongArity(out, "hgetall");
            return;
        }

        byte[] key = cmd.toByteArray(1);
        int count = db.hgetallReplyCount(key);
        out.arrayHeader(count);
        if (count == 0) {
            return;
        }
        bulkOut.reset(out);
        db.hgetallReplyInto(key, bulkOut);
    }

    private void hlen(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            wrongArity(out, "hlen");
            return;
        }
        out.integer(db.hlen(cmd.toByteArray(1)));
    }

    private void hdel(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 3) {
            wrongArity(out, "hdel");
            return;
        }
        int fieldsLen = cmd.argc() - 2;
        sliceResetFromCommand(cmd, 2, fieldsLen);
        try {
            out.integer(db.hdel(cmd.toByteArray(1), slice));
        } finally {
            clearScratch(fieldsLen);
        }
    }

    private void sadd(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 3) {
            wrongArity(out, "sadd");
            return;
        }
        long extra = (long) Math.max(0, cmd.len(1)) + 16L;
        for (int i = 2; i < cmd.argc(); i++) {
            extra += Math.max(0, cmd.len(i));
        }
        db.ensureWriteAllowed(extra);
        int membersLen = cmd.argc() - 2;
        sliceResetFromCommand(cmd, 2, membersLen);
        try {
            out.integer(db.sadd(cmd.toByteArray(1), slice));
            db.enforceMaxmemory();
        } finally {
            clearScratch(membersLen);
        }
    }

    private void srem(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 3) {
            wrongArity(out, "srem");
            return;
        }
        int membersLen = cmd.argc() - 2;
        sliceResetFromCommand(cmd, 2, membersLen);
        try {
            out.integer(db.srem(cmd.toByteArray(1), slice));
        } finally {
            clearScratch(membersLen);
        }
    }

    private void smembers(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            wrongArity(out, "smembers");
            return;
        }

        byte[] key = cmd.toByteArray(1);
        int count = db.smembersReplyCount(key);
        out.arrayHeader(count);
        if (count == 0) {
            return;
        }
        bulkOut.reset(out);
        db.smembersReplyInto(key, bulkOut);
    }

    private void sismember(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            wrongArity(out, "sismember");
            return;
        }
        out.integer(db.sismember(cmd.toByteArray(1), cmd.toByteArray(2)) ? 1 : 0);
    }

    private void scard(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            wrongArity(out, "scard");
            return;
        }
        out.integer(db.scard(cmd.toByteArray(1)));
    }

    private void zadd(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 4) {
            wrongArity(out, "zadd");
            return;
        }
        long extra = (long) Math.max(0, cmd.len(1)) + 16L;
        for (int i = 2; i < cmd.argc(); i++) {
            extra += Math.max(0, cmd.len(i));
        }
        db.ensureWriteAllowed(extra);
        int pairsLen = cmd.argc() - 2;
        sliceResetFromCommand(cmd, 2, pairsLen);
        try {
            out.integer(db.zadd(cmd.toByteArray(1), slice));
            db.enforceMaxmemory();
        } finally {
            clearScratch(pairsLen);
        }
    }

    private void zrange(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 4 || cmd.argc() > 6) {
            wrongArity(out, "zrange");
            return;
        }
        long start = parseLong(cmd, 2, "start");
        long stop = parseLong(cmd, 3, "stop");

        boolean withScores = false;
        boolean rev = false;
        for (int i = 4; i < cmd.argc(); i++) {
            if (asciiEqualsIgnoreCase(cmd, i, "WITHSCORES")) {
                withScores = true;
                continue;
            }
            if (asciiEqualsIgnoreCase(cmd, i, "REV")) {
                rev = true;
                continue;
            }
            out.error("ERR syntax error");
            return;
        }

        byte[] key = cmd.toByteArray(1);
        int count = rev
                ? db.zrevrangeReplyCount(key, start, stop, withScores)
                : db.zrangeReplyCount(key, start, stop, withScores);
        out.arrayHeader(count);
        if (count == 0) {
            return;
        }
        bulkOut.reset(out);
        if (rev) {
            db.zrevrangeReplyInto(key, start, stop, withScores, bulkOut);
        } else {
            db.zrangeReplyInto(key, start, stop, withScores, bulkOut);
        }
    }

    private void zrevrange(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 4 && cmd.argc() != 5) {
            wrongArity(out, "zrevrange");
            return;
        }
        long start = parseLong(cmd, 2, "start");
        long stop = parseLong(cmd, 3, "stop");

        boolean withScores = false;
        if (cmd.argc() == 5) {
            if (!asciiEqualsIgnoreCase(cmd, 4, "WITHSCORES")) {
                out.error("ERR syntax error");
                return;
            }
            withScores = true;
        }

        byte[] key = cmd.toByteArray(1);
        int count = db.zrevrangeReplyCount(key, start, stop, withScores);
        out.arrayHeader(count);
        if (count == 0) {
            return;
        }
        bulkOut.reset(out);
        db.zrevrangeReplyInto(key, start, stop, withScores, bulkOut);
    }

    private void zrangebyscore(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 4) {
            wrongArity(out, "zrangebyscore");
            return;
        }

        ScoreBound min = parseScoreBound(cmd.toByteArray(2));
        ScoreBound max = parseScoreBound(cmd.toByteArray(3));

        boolean withScores = false;
        long offset = 0;
        long count = Long.MAX_VALUE;

        int i = 4;
        while (i < cmd.argc()) {
            if (asciiEqualsIgnoreCase(cmd, i, "WITHSCORES")) {
                withScores = true;
                i++;
                continue;
            }
            if (asciiEqualsIgnoreCase(cmd, i, "LIMIT")) {
                if (i + 2 >= cmd.argc()) {
                    out.error("ERR syntax error");
                    return;
                }
                offset = parseNonNegativeLong(cmd, i + 1, "offset");
                count = parseNonNegativeLong(cmd, i + 2, "count");
                i += 3;
                continue;
            }
            out.error("ERR syntax error");
            return;
        }

        byte[] key = cmd.toByteArray(1);
        int replyCount = db.zrangeByScoreReplyCount(key, min.value, min.exclusive, max.value, max.exclusive, withScores, offset, count);
        out.arrayHeader(replyCount);
        if (replyCount == 0) {
            return;
        }
        bulkOut.reset(out);
        db.zrangeByScoreReplyInto(key, min.value, min.exclusive, max.value, max.exclusive, withScores, offset, count, bulkOut);
    }

    private void zremrangebyscore(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 4) {
            wrongArity(out, "zremrangebyscore");
            return;
        }

        ScoreBound min = parseScoreBound(cmd.toByteArray(2));
        ScoreBound max = parseScoreBound(cmd.toByteArray(3));
        out.integer(db.zremrangeByScore(cmd.toByteArray(1), min.value, min.exclusive, max.value, max.exclusive));
    }

    private void zrevrangebyscore(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 4) {
            wrongArity(out, "zrevrangebyscore");
            return;
        }

        ScoreBound max = parseScoreBound(cmd.toByteArray(2));
        ScoreBound min = parseScoreBound(cmd.toByteArray(3));

        boolean withScores = false;
        long offset = 0;
        long count = Long.MAX_VALUE;

        int i = 4;
        while (i < cmd.argc()) {
            if (asciiEqualsIgnoreCase(cmd, i, "WITHSCORES")) {
                withScores = true;
                i++;
                continue;
            }
            if (asciiEqualsIgnoreCase(cmd, i, "LIMIT")) {
                if (i + 2 >= cmd.argc()) {
                    out.error("ERR syntax error");
                    return;
                }
                offset = parseNonNegativeLong(cmd, i + 1, "offset");
                count = parseNonNegativeLong(cmd, i + 2, "count");
                i += 3;
                continue;
            }
            out.error("ERR syntax error");
            return;
        }

        byte[] key = cmd.toByteArray(1);
        int replyCount = db.zrevrangeByScoreReplyCount(key, min.value, min.exclusive, max.value, max.exclusive, withScores, offset, count);
        out.arrayHeader(replyCount);
        if (replyCount == 0) {
            return;
        }
        bulkOut.reset(out);
        db.zrevrangeByScoreReplyInto(key, min.value, min.exclusive, max.value, max.exclusive, withScores, offset, count, bulkOut);
    }

    private static final class WriterBulkStringOutput implements YierdisBulkStringOutput {
        private RespWriter writer;

        void reset(RespWriter writer) {
            this.writer = writer;
        }

        @Override
        public void bulkString(byte[] buf, int off, int len) {
            writer.bulkString(buf, off, len);
        }

        @Override
        public void bulkString(YierdisOffHeapSlice slice) {
            writer.bulkString(slice);
        }

        @Override
        public void bulkStringNull() {
            writer.bulkString((byte[]) null);
        }

        @Override
        public void bulkStringLongAscii(long value) {
            writer.bulkStringLongAscii(value);
        }
    }

    private void zremrangebyrank(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 4) {
            wrongArity(out, "zremrangebyrank");
            return;
        }
        long start = parseLong(cmd, 2, "start");
        long stop = parseLong(cmd, 3, "stop");
        out.integer(db.zremrangeByRank(cmd.toByteArray(1), start, stop));
    }

    private void zrem(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 3) {
            wrongArity(out, "zrem");
            return;
        }
        int membersLen = cmd.argc() - 2;
        sliceResetFromCommand(cmd, 2, membersLen);
        try {
            out.integer(db.zrem(cmd.toByteArray(1), slice));
        } finally {
            clearScratch(membersLen);
        }
    }

    private static void wrongArity(RespWriter out, String cmdLower) {
        out.error("ERR wrong number of arguments for '" + cmdLower + "' command");
    }

    private void sliceResetFromCommand(RespCommand cmd, int argStart, int len) {
        if (len < 0) {
            throw new IllegalArgumentException("len must be non-negative");
        }
        if (len == 0) {
            slice.reset(argvScratch, 0, 0);
            return;
        }
        ensureScratchCapacity(len);
        for (int i = 0; i < len; i++) {
            argvScratch[i] = cmd.toByteArray(argStart + i);
        }
        slice.reset(argvScratch, 0, len);
    }

    private void clearScratch(int len) {
        if (len <= 0) {
            slice.reset(argvScratch, 0, 0);
            return;
        }
        Arrays.fill(argvScratch, 0, Math.min(len, argvScratch.length), null);
        slice.reset(argvScratch, 0, 0);
    }

    private void ensureScratchCapacity(int desired) {
        if (argvScratch.length >= desired) {
            return;
        }
        int next = argvScratch.length;
        while (next < desired) {
            next <<= 1;
        }
        argvScratch = Arrays.copyOf(argvScratch, next);
    }

    private static String utf8(RespCommand cmd, int argIndex) {
        return utf8(cmd.toByteArray(argIndex));
    }

    private static String utf8(byte[] s) {
        return s == null ? null : new String(s, StandardCharsets.UTF_8);
    }

    private static boolean asciiEqualsIgnoreCase(RespCommand cmd, int argIndex, String literal) {
        if (literal == null) {
            return false;
        }
        if (cmd.isNull(argIndex)) {
            return false;
        }
        int len = cmd.len(argIndex);
        if (len != literal.length()) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            int b = cmd.byteAt(argIndex, i) & 0xFF;
            int c = literal.charAt(i);
            if (b >= 'A' && b <= 'Z') {
                b |= 0x20;
            }
            if (c >= 'A' && c <= 'Z') {
                c |= 0x20;
            }
            if (b != c) {
                return false;
            }
        }
        return true;
    }

    private static boolean asciiEqualsIgnoreCase(byte[] raw, String literal) {
        if (raw == null || literal == null) {
            return false;
        }
        int len = raw.length;
        if (len != literal.length()) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            int b = raw[i] & 0xFF;
            int c = literal.charAt(i);
            if (b >= 'A' && b <= 'Z') {
                b |= 0x20;
            }
            if (c >= 'A' && c <= 'Z') {
                c |= 0x20;
            }
            if (b != c) {
                return false;
            }
        }
        return true;
    }

    private static boolean asciiSliceEqualsIgnoreCase(byte[] raw, int off, int len, String literal) {
        if (raw == null || literal == null) {
            return false;
        }
        if (len != literal.length()) {
            return false;
        }
        if (off < 0 || len < 0 || off + len > raw.length) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            int b = raw[off + i] & 0xFF;
            int c = literal.charAt(i);
            if (b >= 'A' && b <= 'Z') {
                b |= 0x20;
            }
            if (c >= 'A' && c <= 'Z') {
                c |= 0x20;
            }
            if (b != c) {
                return false;
            }
        }
        return true;
    }

    private static long parseLong(RespCommand cmd, int argIndex, String label) {
        if (cmd.isNull(argIndex) || cmd.len(argIndex) == 0) {
            throw new IllegalArgumentException("value is not an integer or out of range: " + label);
        }

        int len = cmd.len(argIndex);
        int i = 0;
        boolean negative = false;
        byte first = cmd.byteAt(argIndex, 0);
        if (first == '-' || first == '+') {
            negative = first == '-';
            i = 1;
            if (i == len) {
                throw new IllegalArgumentException("value is not an integer or out of range: " + label);
            }
        }

        long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        long multMin = limit / 10;
        long result = 0;

        while (i < len) {
            int digit = cmd.byteAt(argIndex, i++) - '0';
            if (digit < 0 || digit > 9) {
                throw new IllegalArgumentException("value is not an integer or out of range: " + label);
            }
            if (result < multMin) {
                throw new IllegalArgumentException("value is not an integer or out of range: " + label);
            }
            result *= 10;
            if (result < limit + digit) {
                throw new IllegalArgumentException("value is not an integer or out of range: " + label);
            }
            result -= digit;
        }

        return negative ? result : -result;
    }

    private static long parseNonNegativeLong(RespCommand cmd, int argIndex, String label) {
        long v = parseLong(cmd, argIndex, label);
        if (v < 0) {
            throw new IllegalArgumentException("value is not an integer or out of range: " + label);
        }
        return v;
    }

    private static int parseIntClamped(RespCommand cmd, int argIndex, String label) {
        long v = parseLong(cmd, argIndex, label);
        if (v > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        if (v < Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        }
        return (int) v;
    }

    private static long parseLong(byte[] s, String label) {
        if (s == null || s.length == 0) {
            throw new IllegalArgumentException("value is not an integer or out of range: " + label);
        }

        int i = 0;
        boolean negative = false;
        byte first = s[0];
        if (first == '-' || first == '+') {
            negative = first == '-';
            i = 1;
            if (i == s.length) {
                throw new IllegalArgumentException("value is not an integer or out of range: " + label);
            }
        }

        long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        long multMin = limit / 10;
        long result = 0;

        while (i < s.length) {
            int digit = s[i++] - '0';
            if (digit < 0 || digit > 9) {
                throw new IllegalArgumentException("value is not an integer or out of range: " + label);
            }
            if (result < multMin) {
                throw new IllegalArgumentException("value is not an integer or out of range: " + label);
            }
            result *= 10;
            if (result < limit + digit) {
                throw new IllegalArgumentException("value is not an integer or out of range: " + label);
            }
            result -= digit;
        }

        return negative ? result : -result;
    }

    private static long parseNonNegativeLong(byte[] s, String label) {
        long v = parseLong(s, label);
        if (v < 0) {
            throw new IllegalArgumentException("value is not an integer or out of range: " + label);
        }
        return v;
    }

    private static int parseIntClamped(byte[] s, String label) {
        long v = parseLong(s, label);
        if (v > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        if (v < Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        }
        return (int) v;
    }

    private static ScoreBound parseScoreBound(byte[] raw) {
        if (raw == null || raw.length == 0) {
            throw new YierdisDb.YierdisCommandException("ERR min or max is not a float");
        }

        int start = 0;
        boolean exclusive = false;
        byte first = raw[0];
        if (first == '(') {
            exclusive = true;
            start = 1;
        } else if (first == '[') {
            start = 1;
        }
        if (start >= raw.length) {
            throw new YierdisDb.YierdisCommandException("ERR min or max is not a float");
        }

        int len = raw.length - start;
        if (len == 4 && raw[start] == '-' && asciiSliceEqualsIgnoreCase(raw, start + 1, 3, "INF")) {
            return new ScoreBound(Double.NEGATIVE_INFINITY, exclusive);
        }
        if (len == 4 && raw[start] == '+' && asciiSliceEqualsIgnoreCase(raw, start + 1, 3, "INF")) {
            return new ScoreBound(Double.POSITIVE_INFINITY, exclusive);
        }
        if (len == 3 && asciiSliceEqualsIgnoreCase(raw, start, 3, "INF")) {
            return new ScoreBound(Double.POSITIVE_INFINITY, exclusive);
        }

        String s = new String(raw, start, len, StandardCharsets.US_ASCII);
        double v;
        try {
            v = Double.parseDouble(s);
        } catch (NumberFormatException e) {
            throw new YierdisDb.YierdisCommandException("ERR min or max is not a float");
        }
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            throw new YierdisDb.YierdisCommandException("ERR min or max is not a float");
        }
        return new ScoreBound(v, exclusive);
    }

    private static final class ScoreBound {
        final double value;
        final boolean exclusive;

        private ScoreBound(double value, boolean exclusive) {
            this.value = value;
            this.exclusive = exclusive;
        }
    }

    private static final class ByteArraySliceList extends AbstractList<byte[]> implements RandomAccess {
        private byte[][] argv;
        private int offset;
        private int len;

        void reset(byte[][] argv, int offset, int len) {
            this.argv = argv;
            this.offset = offset;
            this.len = len;
        }

        @Override
        public byte[] get(int index) {
            if (index < 0 || index >= len) {
                throw new IndexOutOfBoundsException();
            }
            return argv[offset + index];
        }

        @Override
        public int size() {
            return len;
        }
    }

    private static final class RespCommandArgBytesView implements YierdisBytesView {
        private RespCommand cmd;
        private int argIndex;

        RespCommandArgBytesView reset(RespCommand cmd, int argIndex) {
            this.cmd = cmd;
            this.argIndex = argIndex;
            return this;
        }

        @Override
        public int len() {
            return cmd.len(argIndex);
        }

        @Override
        public byte byteAt(int index) {
            return cmd.byteAt(argIndex, index);
        }
    }
}
