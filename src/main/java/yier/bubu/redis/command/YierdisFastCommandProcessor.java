package yier.bubu.redis.command;

import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.ValueType;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespWriter;

import java.nio.charset.StandardCharsets;
import java.util.AbstractList;
import java.util.List;
import java.util.Locale;
import java.util.RandomAccess;
import java.util.concurrent.TimeUnit;

/**
 * A server-side command processor optimized for low allocation.
 * <p>
 * It executes commands and writes RESP2 replies directly via {@link RespWriter}.
 */
public final class YierdisFastCommandProcessor {
    private static final byte[] HELLO_SERVER_KEY = "server".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_SERVER_VALUE = "yierdis".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_VERSION_KEY = "version".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_VERSION_VALUE = "0.1.0".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_PROTO_KEY = "proto".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_PROTO_VALUE = "2".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_MODE_KEY = "mode".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_MODE_VALUE = "standalone".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_ROLE_KEY = "role".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_ROLE_VALUE = "master".getBytes(StandardCharsets.US_ASCII);

    private final YierdisDb db;
    private final ByteArraySliceList slice = new ByteArraySliceList();

    public YierdisFastCommandProcessor(YierdisDb db) {
        this.db = db;
    }

    public void execute(RespCommand cmd, RespWriter out) {
        int argc = cmd.argc();
        if (argc <= 0) {
            out.error("ERR empty command");
            return;
        }
        byte[] rawCmd = cmd.arg(0);
        if (rawCmd == null || rawCmd.length == 0) {
            out.error("ERR empty command");
            return;
        }

        try {
            if (asciiEqualsIgnoreCase(rawCmd, "PING")) {
                ping(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "ECHO")) {
                echo(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "HELLO")) {
                hello(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "COMMAND")) {
                out.emptyArray();
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "SELECT")) {
                select(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "FLUSHDB")) {
                db.flushDb();
                out.simpleString("OK");
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "TYPE")) {
                type(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "KEYS")) {
                keys(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "DEL")) {
                del(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "EXISTS")) {
                exists(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "SET")) {
                set(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "GET")) {
                get(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "STRLEN")) {
                strlen(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "APPEND")) {
                append(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "INCR")) {
                incrBy(cmd, out, 1);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "DECR")) {
                incrBy(cmd, out, -1);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "EXPIRE")) {
                expire(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "TTL")) {
                ttl(cmd, out);
                return;
            }

            if (asciiEqualsIgnoreCase(rawCmd, "LPUSH")) {
                push(cmd, out, true);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "RPUSH")) {
                push(cmd, out, false);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "LRANGE")) {
                lrange(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "LPOP")) {
                pop(cmd, out, true);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "RPOP")) {
                pop(cmd, out, false);
                return;
            }

            if (asciiEqualsIgnoreCase(rawCmd, "HSET")) {
                hset(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "HGET")) {
                hget(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "HGETALL")) {
                hgetall(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "HLEN")) {
                hlen(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "HDEL")) {
                hdel(cmd, out);
                return;
            }

            if (asciiEqualsIgnoreCase(rawCmd, "SADD")) {
                sadd(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "SREM")) {
                srem(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "SMEMBERS")) {
                smembers(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "SISMEMBER")) {
                sismember(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "SCARD")) {
                scard(cmd, out);
                return;
            }

            if (asciiEqualsIgnoreCase(rawCmd, "ZADD")) {
                zadd(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "ZRANGE")) {
                zrange(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "ZREVRANGE")) {
                zrevrange(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "ZRANGEBYSCORE")) {
                zrangebyscore(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "ZREVRANGEBYSCORE")) {
                zrevrangebyscore(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "ZREMRANGEBYSCORE")) {
                zremrangebyscore(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "ZREMRANGEBYRANK")) {
                zremrangebyrank(cmd, out);
                return;
            }
            if (asciiEqualsIgnoreCase(rawCmd, "ZREM")) {
                zrem(cmd, out);
                return;
            }

            out.error("ERR unknown command '" + utf8(rawCmd) + "'");
        } catch (YierdisDb.WrongTypeException e) {
            out.error(e.getMessage());
        } catch (YierdisDb.YierdisCommandException e) {
            out.error(e.getMessage());
        } catch (IllegalArgumentException e) {
            out.error("ERR " + e.getMessage());
        }
    }

    private void ping(RespCommand cmd, RespWriter out) {
        if (cmd.argc() == 1) {
            out.simpleString("PONG");
            return;
        }
        if (cmd.argc() == 2) {
            out.bulkString(cmd.arg(1));
            return;
        }
        wrongArity(out, "ping");
    }

    private void echo(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            wrongArity(out, "echo");
            return;
        }
        out.bulkString(cmd.arg(1));
    }

    private void hello(RespCommand cmd, RespWriter out) {
        // Minimal RESP2-friendly HELLO implementation.
        String version = cmd.argc() >= 2 ? utf8(cmd.arg(1)) : "2";
        if ("3".equals(version)) {
            out.error("ERR RESP3 is not supported (use HELLO 2 / redis-cli --resp2)");
            return;
        }
        if (!"2".equals(version)) {
            out.error("ERR unsupported protocol version");
            return;
        }

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
        byte[] index = cmd.arg(1);
        if (index != null && index.length == 1 && index[0] == '0') {
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
        ValueType t = db.typeOf(cmd.arg(1));
        if (t == null) {
            out.simpleString("none");
            return;
        }
        out.simpleString(t.name().toLowerCase(Locale.ROOT));
    }

    private void keys(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            wrongArity(out, "keys");
            return;
        }
        out.bulkStringArray(db.keys(cmd.arg(1)));
    }

    private void del(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 2) {
            wrongArity(out, "del");
            return;
        }
        slice.reset(cmd.argvUnsafe(), 1, cmd.argc() - 1);
        out.integer(db.del(slice));
    }

    private void exists(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 2) {
            wrongArity(out, "exists");
            return;
        }
        slice.reset(cmd.argvUnsafe(), 1, cmd.argc() - 1);
        out.integer(db.exists(slice));
    }

    private void set(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 3) {
            wrongArity(out, "set");
            return;
        }

        byte[] key = cmd.arg(1);
        byte[] value = cmd.arg(2);

        YierdisDb.SetMode mode = YierdisDb.SetMode.NORMAL;
        YierdisDb.ExpireOption expire = null;

        for (int i = 3; i < cmd.argc(); i++) {
            byte[] opt = cmd.arg(i);
            if (asciiEqualsIgnoreCase(opt, "NX")) {
                mode = YierdisDb.SetMode.NX;
                continue;
            }
            if (asciiEqualsIgnoreCase(opt, "XX")) {
                mode = YierdisDb.SetMode.XX;
                continue;
            }
            if (asciiEqualsIgnoreCase(opt, "EX") && i + 1 < cmd.argc()) {
                long seconds = parseLong(cmd.arg(++i), "seconds");
                expire = new YierdisDb.ExpireOption(TimeUnit.SECONDS, seconds);
                continue;
            }
            if (asciiEqualsIgnoreCase(opt, "PX") && i + 1 < cmd.argc()) {
                long millis = parseLong(cmd.arg(++i), "milliseconds");
                expire = new YierdisDb.ExpireOption(TimeUnit.MILLISECONDS, millis);
                continue;
            }
            out.error("ERR syntax error");
            return;
        }

        boolean ok = db.setString(key, value, mode, expire);
        if (!ok) {
            out.bulkString(null);
            return;
        }
        out.simpleString("OK");
    }

    private void get(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            wrongArity(out, "get");
            return;
        }
        out.bulkString(db.getStringBytes(cmd.arg(1)));
    }

    private void strlen(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            wrongArity(out, "strlen");
            return;
        }
        out.integer(db.strlen(cmd.arg(1)));
    }

    private void append(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            wrongArity(out, "append");
            return;
        }
        out.integer(db.append(cmd.arg(1), cmd.arg(2)));
    }

    private void incrBy(RespCommand cmd, RespWriter out, long delta) {
        if (cmd.argc() != 2) {
            wrongArity(out, delta > 0 ? "incr" : "decr");
            return;
        }
        out.integer(db.incrBy(cmd.arg(1), delta));
    }

    private void expire(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            wrongArity(out, "expire");
            return;
        }
        long seconds = parseLong(cmd.arg(2), "seconds");
        out.integer(db.expire(cmd.arg(1), seconds) ? 1 : 0);
    }

    private void ttl(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            wrongArity(out, "ttl");
            return;
        }
        out.integer(db.ttlSeconds(cmd.arg(1)));
    }

    private void push(RespCommand cmd, RespWriter out, boolean left) {
        if (cmd.argc() < 3) {
            wrongArity(out, left ? "lpush" : "rpush");
            return;
        }
        slice.reset(cmd.argvUnsafe(), 2, cmd.argc() - 2);
        int len = left ? db.lpush(cmd.arg(1), slice) : db.rpush(cmd.arg(1), slice);
        out.integer(len);
    }

    private void lrange(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 4) {
            wrongArity(out, "lrange");
            return;
        }
        int start = parseIntClamped(cmd.arg(2), "start");
        int stop = parseIntClamped(cmd.arg(3), "stop");
        out.bulkStringArray(db.lrange(cmd.arg(1), start, stop));
    }

    private void pop(RespCommand cmd, RespWriter out, boolean left) {
        if (cmd.argc() != 2 && cmd.argc() != 3) {
            wrongArity(out, left ? "lpop" : "rpop");
            return;
        }
        int count = 1;
        boolean hasCount = cmd.argc() == 3;
        if (hasCount) {
            count = parseIntClamped(cmd.arg(2), "count");
        }

        List<byte[]> popped = left ? db.lpop(cmd.arg(1), count) : db.rpop(cmd.arg(1), count);
        popResponse(out, popped, hasCount);
    }

    private static void popResponse(RespWriter out, List<byte[]> popped, boolean hasCount) {
        if (!hasCount) {
            if (popped.isEmpty()) {
                out.bulkString(null);
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
        slice.reset(cmd.argvUnsafe(), 2, cmd.argc() - 2);
        out.integer(db.hset(cmd.arg(1), slice));
    }

    private void hget(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            wrongArity(out, "hget");
            return;
        }
        out.bulkString(db.hget(cmd.arg(1), cmd.arg(2)));
    }

    private void hgetall(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            wrongArity(out, "hgetall");
            return;
        }
        out.bulkStringArray(db.hgetall(cmd.arg(1)));
    }

    private void hlen(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            wrongArity(out, "hlen");
            return;
        }
        out.integer(db.hlen(cmd.arg(1)));
    }

    private void hdel(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 3) {
            wrongArity(out, "hdel");
            return;
        }
        slice.reset(cmd.argvUnsafe(), 2, cmd.argc() - 2);
        out.integer(db.hdel(cmd.arg(1), slice));
    }

    private void sadd(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 3) {
            wrongArity(out, "sadd");
            return;
        }
        slice.reset(cmd.argvUnsafe(), 2, cmd.argc() - 2);
        out.integer(db.sadd(cmd.arg(1), slice));
    }

    private void srem(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 3) {
            wrongArity(out, "srem");
            return;
        }
        slice.reset(cmd.argvUnsafe(), 2, cmd.argc() - 2);
        out.integer(db.srem(cmd.arg(1), slice));
    }

    private void smembers(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            wrongArity(out, "smembers");
            return;
        }
        out.bulkStringArray(db.smembers(cmd.arg(1)));
    }

    private void sismember(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            wrongArity(out, "sismember");
            return;
        }
        out.integer(db.sismember(cmd.arg(1), cmd.arg(2)) ? 1 : 0);
    }

    private void scard(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            wrongArity(out, "scard");
            return;
        }
        out.integer(db.scard(cmd.arg(1)));
    }

    private void zadd(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 4) {
            wrongArity(out, "zadd");
            return;
        }
        slice.reset(cmd.argvUnsafe(), 2, cmd.argc() - 2);
        out.integer(db.zadd(cmd.arg(1), slice));
    }

    private void zrange(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 4 || cmd.argc() > 6) {
            wrongArity(out, "zrange");
            return;
        }
        long start = parseLong(cmd.arg(2), "start");
        long stop = parseLong(cmd.arg(3), "stop");

        boolean withScores = false;
        boolean rev = false;
        for (int i = 4; i < cmd.argc(); i++) {
            byte[] opt = cmd.arg(i);
            if (asciiEqualsIgnoreCase(opt, "WITHSCORES")) {
                withScores = true;
                continue;
            }
            if (asciiEqualsIgnoreCase(opt, "REV")) {
                rev = true;
                continue;
            }
            out.error("ERR syntax error");
            return;
        }

        out.bulkStringArray(rev
                ? db.zrevrange(cmd.arg(1), start, stop, withScores)
                : db.zrange(cmd.arg(1), start, stop, withScores));
    }

    private void zrevrange(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 4 && cmd.argc() != 5) {
            wrongArity(out, "zrevrange");
            return;
        }
        long start = parseLong(cmd.arg(2), "start");
        long stop = parseLong(cmd.arg(3), "stop");

        boolean withScores = false;
        if (cmd.argc() == 5) {
            if (!asciiEqualsIgnoreCase(cmd.arg(4), "WITHSCORES")) {
                out.error("ERR syntax error");
                return;
            }
            withScores = true;
        }

        out.bulkStringArray(db.zrevrange(cmd.arg(1), start, stop, withScores));
    }

    private void zrangebyscore(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 4) {
            wrongArity(out, "zrangebyscore");
            return;
        }

        ScoreBound min = parseScoreBound(cmd.arg(2));
        ScoreBound max = parseScoreBound(cmd.arg(3));

        boolean withScores = false;
        long offset = 0;
        long count = Long.MAX_VALUE;

        int i = 4;
        while (i < cmd.argc()) {
            byte[] opt = cmd.arg(i);
            if (asciiEqualsIgnoreCase(opt, "WITHSCORES")) {
                withScores = true;
                i++;
                continue;
            }
            if (asciiEqualsIgnoreCase(opt, "LIMIT")) {
                if (i + 2 >= cmd.argc()) {
                    out.error("ERR syntax error");
                    return;
                }
                offset = parseNonNegativeLong(cmd.arg(i + 1), "offset");
                count = parseNonNegativeLong(cmd.arg(i + 2), "count");
                i += 3;
                continue;
            }
            out.error("ERR syntax error");
            return;
        }

        out.bulkStringArray(db.zrangeByScore(cmd.arg(1), min.value, min.exclusive, max.value, max.exclusive, withScores, offset, count));
    }

    private void zremrangebyscore(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 4) {
            wrongArity(out, "zremrangebyscore");
            return;
        }

        ScoreBound min = parseScoreBound(cmd.arg(2));
        ScoreBound max = parseScoreBound(cmd.arg(3));
        out.integer(db.zremrangeByScore(cmd.arg(1), min.value, min.exclusive, max.value, max.exclusive));
    }

    private void zrevrangebyscore(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 4) {
            wrongArity(out, "zrevrangebyscore");
            return;
        }

        ScoreBound max = parseScoreBound(cmd.arg(2));
        ScoreBound min = parseScoreBound(cmd.arg(3));

        boolean withScores = false;
        long offset = 0;
        long count = Long.MAX_VALUE;

        int i = 4;
        while (i < cmd.argc()) {
            byte[] opt = cmd.arg(i);
            if (asciiEqualsIgnoreCase(opt, "WITHSCORES")) {
                withScores = true;
                i++;
                continue;
            }
            if (asciiEqualsIgnoreCase(opt, "LIMIT")) {
                if (i + 2 >= cmd.argc()) {
                    out.error("ERR syntax error");
                    return;
                }
                offset = parseNonNegativeLong(cmd.arg(i + 1), "offset");
                count = parseNonNegativeLong(cmd.arg(i + 2), "count");
                i += 3;
                continue;
            }
            out.error("ERR syntax error");
            return;
        }

        out.bulkStringArray(db.zrevrangeByScore(
                cmd.arg(1),
                min.value, min.exclusive,
                max.value, max.exclusive,
                withScores,
                offset,
                count
        ));
    }

    private void zremrangebyrank(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 4) {
            wrongArity(out, "zremrangebyrank");
            return;
        }
        long start = parseLong(cmd.arg(2), "start");
        long stop = parseLong(cmd.arg(3), "stop");
        out.integer(db.zremrangeByRank(cmd.arg(1), start, stop));
    }

    private void zrem(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 3) {
            wrongArity(out, "zrem");
            return;
        }
        slice.reset(cmd.argvUnsafe(), 2, cmd.argc() - 2);
        out.integer(db.zrem(cmd.arg(1), slice));
    }

    private static void wrongArity(RespWriter out, String cmdLower) {
        out.error("ERR wrong number of arguments for '" + cmdLower + "' command");
    }

    private static String utf8(byte[] s) {
        return s == null ? null : new String(s, StandardCharsets.UTF_8);
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
}
