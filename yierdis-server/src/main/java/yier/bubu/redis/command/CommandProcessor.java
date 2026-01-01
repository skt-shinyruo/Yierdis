package yier.bubu.redis.command;

import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.ValueType;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapOutOfMemoryException;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public final class CommandProcessor {
    private final YierdisDb db;

    public CommandProcessor(YierdisDb db) {
        this.db = db;
    }

    public RespObject execute(List<byte[]> args) {
        if (args == null || args.isEmpty() || args.get(0) == null || args.get(0).length == 0) {
            return RespError.of("ERR empty command");
        }

        byte[] cmd = args.get(0);
        try {
            if (asciiEqualsIgnoreCase(cmd, "PING")) {
                return ping(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "ECHO")) {
                return echo(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "HELLO")) {
                return hello(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "COMMAND")) {
                return RespArray.empty();
            }
            if (asciiEqualsIgnoreCase(cmd, "SELECT")) {
                return select(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "FLUSHDB")) {
                db.flushDb();
                return RespSimpleString.of("OK");
            }
            if (asciiEqualsIgnoreCase(cmd, "TYPE")) {
                return type(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "MEMORY")) {
                return memory(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "OBJECT")) {
                return object(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "KEYS")) {
                return keys(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "DEL")) {
                return del(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "EXISTS")) {
                return exists(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "SET")) {
                return set(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "GET")) {
                return get(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "STRLEN")) {
                return strlen(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "APPEND")) {
                return append(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "INCR")) {
                return incrBy(args, 1);
            }
            if (asciiEqualsIgnoreCase(cmd, "DECR")) {
                return incrBy(args, -1);
            }
            if (asciiEqualsIgnoreCase(cmd, "EXPIRE")) {
                return expire(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "TTL")) {
                return ttl(args);
            }

            if (asciiEqualsIgnoreCase(cmd, "LPUSH")) {
                return lpush(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "RPUSH")) {
                return rpush(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "LRANGE")) {
                return lrange(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "LPOP")) {
                return lpop(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "RPOP")) {
                return rpop(args);
            }

            if (asciiEqualsIgnoreCase(cmd, "HSET")) {
                return hset(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "HGET")) {
                return hget(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "HGETALL")) {
                return hgetall(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "HLEN")) {
                return hlen(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "HDEL")) {
                return hdel(args);
            }

            if (asciiEqualsIgnoreCase(cmd, "SADD")) {
                return sadd(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "SREM")) {
                return srem(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "SMEMBERS")) {
                return smembers(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "SISMEMBER")) {
                return sismember(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "SCARD")) {
                return scard(args);
            }

            if (asciiEqualsIgnoreCase(cmd, "ZADD")) {
                return zadd(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "ZRANGE")) {
                return zrange(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "ZREVRANGE")) {
                return zrevrange(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "ZRANGEBYSCORE")) {
                return zrangebyscore(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "ZREVRANGEBYSCORE")) {
                return zrevrangebyscore(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "ZREMRANGEBYSCORE")) {
                return zremrangebyscore(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "ZREMRANGEBYRANK")) {
                return zremrangebyrank(args);
            }
            if (asciiEqualsIgnoreCase(cmd, "ZREM")) {
                return zrem(args);
            }

            return RespError.of("ERR unknown command '" + utf8(cmd) + "'");
        } catch (YierdisDb.WrongTypeException e) {
            return RespError.of(e.getMessage());
        } catch (YierdisDb.YierdisCommandException e) {
            return RespError.of(e.getMessage());
        } catch (YierdisOffHeapOutOfMemoryException e) {
            return RespError.of("OOM off-heap memory limit exceeded");
        } catch (IllegalArgumentException e) {
            return RespError.of("ERR " + e.getMessage());
        }
    }

    private RespObject ping(List<byte[]> args) {
        if (args.size() == 1) {
            return RespSimpleString.of("PONG");
        }
        if (args.size() == 2) {
            return RespBulkString.ofBytes(args.get(1));
        }
        return wrongArity("ping");
    }

    private RespObject echo(List<byte[]> args) {
        if (args.size() != 2) {
            return wrongArity("echo");
        }
        return RespBulkString.ofBytes(args.get(1));
    }

    private RespObject hello(List<byte[]> args) {
        // Minimal RESP2-friendly HELLO implementation.
        // Redis returns a map in RESP3 and an array of alternating keys/values in RESP2.
        String version = args.size() >= 2 ? utf8(args.get(1)) : "2";
        if ("3".equals(version)) {
            return RespError.of("ERR RESP3 is not supported (use HELLO 2 / redis-cli --resp2)");
        }
        if (!"2".equals(version)) {
            return RespError.of("ERR unsupported protocol version");
        }

        List<RespObject> out = new ArrayList<>();
        // Alternating key/value pairs
        addKv(out, "server", "yierdis");
        addKv(out, "version", "0.1.0");
        addKv(out, "proto", "2");
        addKv(out, "mode", "standalone");
        addKv(out, "role", "master");
        return RespArray.of(out);
    }

    private static void addKv(List<RespObject> out, String k, String v) {
        out.add(RespBulkString.ofString(k));
        out.add(RespBulkString.ofString(v));
    }

    private RespObject select(List<byte[]> args) {
        if (args.size() != 2) {
            return wrongArity("select");
        }
        byte[] index = args.get(1);
        if (index != null && index.length == 1 && index[0] == '0') {
            return RespSimpleString.of("OK");
        }
        return RespError.of("ERR only DB 0 is supported");
    }

    private RespObject type(List<byte[]> args) {
        if (args.size() != 2) {
            return wrongArity("type");
        }
        ValueType t = db.typeOf(args.get(1));
        if (t == null) {
            return RespSimpleString.of("none");
        }
        return RespSimpleString.of(t.name().toLowerCase(Locale.ROOT));
    }

    private RespObject memory(List<byte[]> args) {
        if (args.size() != 3) {
            return wrongArity("memory");
        }
        if (!asciiEqualsIgnoreCase(args.get(1), "USAGE")) {
            return RespError.of("ERR syntax error");
        }
        long bytes = db.memoryUsage(args.get(2));
        if (bytes < 0) {
            return RespBulkString.nullString();
        }
        return RespInteger.of(bytes);
    }

    private RespObject object(List<byte[]> args) {
        if (args.size() != 3) {
            return wrongArity("object");
        }
        if (!asciiEqualsIgnoreCase(args.get(1), "ENCODING")) {
            return RespError.of("ERR syntax error");
        }
        String enc = db.objectEncoding(args.get(2));
        if (enc == null) {
            return RespBulkString.nullString();
        }
        return RespSimpleString.of(enc);
    }

    private RespObject keys(List<byte[]> args) {
        if (args.size() != 2) {
            return wrongArity("keys");
        }
        return toBulkStringArray(db.keys(args.get(1)));
    }

    private RespObject del(List<byte[]> args) {
        if (args.size() < 2) {
            return wrongArity("del");
        }
        long removed = db.del(args.subList(1, args.size()));
        return RespInteger.of(removed);
    }

    private RespObject exists(List<byte[]> args) {
        if (args.size() < 2) {
            return wrongArity("exists");
        }
        long count = db.exists(args.subList(1, args.size()));
        return RespInteger.of(count);
    }

    private RespObject set(List<byte[]> args) {
        if (args.size() < 3) {
            return wrongArity("set");
        }

        byte[] key = args.get(1);
        byte[] value = args.get(2);

        YierdisDb.SetMode mode = YierdisDb.SetMode.NORMAL;
        YierdisDb.ExpireOption expire = null;

        // SET key value [EX seconds|PX milliseconds] [NX|XX]
        for (int i = 3; i < args.size(); i++) {
            byte[] opt = args.get(i);
            if (asciiEqualsIgnoreCase(opt, "NX")) {
                mode = YierdisDb.SetMode.NX;
                continue;
            }
            if (asciiEqualsIgnoreCase(opt, "XX")) {
                mode = YierdisDb.SetMode.XX;
                continue;
            }
            if (asciiEqualsIgnoreCase(opt, "EX") && i + 1 < args.size()) {
                long seconds = parseLong(args.get(++i), "seconds");
                expire = new YierdisDb.ExpireOption(TimeUnit.SECONDS, seconds);
                continue;
            }
            if (asciiEqualsIgnoreCase(opt, "PX") && i + 1 < args.size()) {
                long millis = parseLong(args.get(++i), "milliseconds");
                expire = new YierdisDb.ExpireOption(TimeUnit.MILLISECONDS, millis);
                continue;
            }
            return RespError.of("ERR syntax error");
        }

        boolean willSet = true;
        if (mode == YierdisDb.SetMode.NX) {
            willSet = db.typeOf(key) == null;
        } else if (mode == YierdisDb.SetMode.XX) {
            willSet = db.typeOf(key) != null;
        }
        if (willSet) {
            long extra = (long) key.length + value.length + 16L;
            db.ensureWriteAllowed(extra);
        }

        boolean ok = db.setString(key, value, mode, expire);
        if (!ok) {
            return RespBulkString.nullString();
        }
        db.enforceMaxmemory();
        return RespSimpleString.of("OK");
    }

    private RespObject get(List<byte[]> args) {
        if (args.size() != 2) {
            return wrongArity("get");
        }
        byte[] value = db.getStringBytes(args.get(1));
        return value == null ? RespBulkString.nullString() : RespBulkString.ofBytes(value);
    }

    private RespObject strlen(List<byte[]> args) {
        if (args.size() != 2) {
            return wrongArity("strlen");
        }
        return RespInteger.of(db.strlen(args.get(1)));
    }

    private RespObject append(List<byte[]> args) {
        if (args.size() != 3) {
            return wrongArity("append");
        }
        long extra = (long) args.get(1).length + args.get(2).length + 16L;
        db.ensureWriteAllowed(extra);
        int len = db.append(args.get(1), args.get(2));
        db.enforceMaxmemory();
        return RespInteger.of(len);
    }

    private RespObject incrBy(List<byte[]> args, long delta) {
        if (args.size() != 2) {
            return wrongArity(delta > 0 ? "incr" : "decr");
        }
        long extra = (long) args.get(1).length + 16L;
        db.ensureWriteAllowed(extra);
        long v = db.incrBy(args.get(1), delta);
        db.enforceMaxmemory();
        return RespInteger.of(v);
    }

    private RespObject expire(List<byte[]> args) {
        if (args.size() != 3) {
            return wrongArity("expire");
        }
        long seconds = parseLong(args.get(2), "seconds");
        return RespInteger.of(db.expire(args.get(1), seconds) ? 1 : 0);
    }

    private RespObject ttl(List<byte[]> args) {
        if (args.size() != 2) {
            return wrongArity("ttl");
        }
        return RespInteger.of(db.ttlSeconds(args.get(1)));
    }

    private RespObject lpush(List<byte[]> args) {
        if (args.size() < 3) {
            return wrongArity("lpush");
        }
        long extra = args.get(1).length + 16L;
        for (int i = 2; i < args.size(); i++) {
            extra += args.get(i).length;
        }
        db.ensureWriteAllowed(extra);
        int len = db.lpush(args.get(1), args.subList(2, args.size()));
        db.enforceMaxmemory();
        return RespInteger.of(len);
    }

    private RespObject rpush(List<byte[]> args) {
        if (args.size() < 3) {
            return wrongArity("rpush");
        }
        long extra = args.get(1).length + 16L;
        for (int i = 2; i < args.size(); i++) {
            extra += args.get(i).length;
        }
        db.ensureWriteAllowed(extra);
        int len = db.rpush(args.get(1), args.subList(2, args.size()));
        db.enforceMaxmemory();
        return RespInteger.of(len);
    }

    private RespObject lrange(List<byte[]> args) {
        if (args.size() != 4) {
            return wrongArity("lrange");
        }
        int start = parseIntClamped(args.get(2), "start");
        int stop = parseIntClamped(args.get(3), "stop");
        List<byte[]> values = db.lrange(args.get(1), start, stop);
        return toBulkStringArray(values);
    }

    private RespObject lpop(List<byte[]> args) {
        if (args.size() != 2 && args.size() != 3) {
            return wrongArity("lpop");
        }
        int count = 1;
        boolean hasCount = args.size() == 3;
        if (hasCount) {
            count = parseIntClamped(args.get(2), "count");
        }
        List<byte[]> popped = db.lpop(args.get(1), count);
        return popResponse(popped, hasCount);
    }

    private RespObject rpop(List<byte[]> args) {
        if (args.size() != 2 && args.size() != 3) {
            return wrongArity("rpop");
        }
        int count = 1;
        boolean hasCount = args.size() == 3;
        if (hasCount) {
            count = parseIntClamped(args.get(2), "count");
        }
        List<byte[]> popped = db.rpop(args.get(1), count);
        return popResponse(popped, hasCount);
    }

    private static RespObject popResponse(List<byte[]> popped, boolean hasCount) {
        if (!hasCount) {
            if (popped.isEmpty()) {
                return RespBulkString.nullString();
            }
            return RespBulkString.ofBytes(popped.get(0));
        }
        return toBulkStringArray(popped);
    }

    private RespObject hset(List<byte[]> args) {
        if (args.size() < 4) {
            return wrongArity("hset");
        }
        long extra = args.get(1).length + 16L;
        for (int i = 2; i < args.size(); i++) {
            extra += args.get(i).length;
        }
        db.ensureWriteAllowed(extra);
        int added = db.hset(args.get(1), args.subList(2, args.size()));
        db.enforceMaxmemory();
        return RespInteger.of(added);
    }

    private RespObject hget(List<byte[]> args) {
        if (args.size() != 3) {
            return wrongArity("hget");
        }
        byte[] v = db.hget(args.get(1), args.get(2));
        return v == null ? RespBulkString.nullString() : RespBulkString.ofBytes(v);
    }

    private RespObject hgetall(List<byte[]> args) {
        if (args.size() != 2) {
            return wrongArity("hgetall");
        }
        return toBulkStringArray(db.hgetall(args.get(1)));
    }

    private RespObject hlen(List<byte[]> args) {
        if (args.size() != 2) {
            return wrongArity("hlen");
        }
        return RespInteger.of(db.hlen(args.get(1)));
    }

    private RespObject hdel(List<byte[]> args) {
        if (args.size() < 3) {
            return wrongArity("hdel");
        }
        return RespInteger.of(db.hdel(args.get(1), args.subList(2, args.size())));
    }

    private RespObject sadd(List<byte[]> args) {
        if (args.size() < 3) {
            return wrongArity("sadd");
        }
        long extra = args.get(1).length + 16L;
        for (int i = 2; i < args.size(); i++) {
            extra += args.get(i).length;
        }
        db.ensureWriteAllowed(extra);
        int added = db.sadd(args.get(1), args.subList(2, args.size()));
        db.enforceMaxmemory();
        return RespInteger.of(added);
    }

    private RespObject srem(List<byte[]> args) {
        if (args.size() < 3) {
            return wrongArity("srem");
        }
        return RespInteger.of(db.srem(args.get(1), args.subList(2, args.size())));
    }

    private RespObject smembers(List<byte[]> args) {
        if (args.size() != 2) {
            return wrongArity("smembers");
        }
        return toBulkStringArray(db.smembers(args.get(1)));
    }

    private RespObject sismember(List<byte[]> args) {
        if (args.size() != 3) {
            return wrongArity("sismember");
        }
        return RespInteger.of(db.sismember(args.get(1), args.get(2)) ? 1 : 0);
    }

    private RespObject scard(List<byte[]> args) {
        if (args.size() != 2) {
            return wrongArity("scard");
        }
        return RespInteger.of(db.scard(args.get(1)));
    }

    private RespObject zadd(List<byte[]> args) {
        if (args.size() < 4) {
            return wrongArity("zadd");
        }
        long extra = args.get(1).length + 16L;
        for (int i = 2; i < args.size(); i++) {
            extra += args.get(i).length;
        }
        db.ensureWriteAllowed(extra);
        int added = db.zadd(args.get(1), args.subList(2, args.size()));
        db.enforceMaxmemory();
        return RespInteger.of(added);
    }

    private RespObject zrange(List<byte[]> args) {
        if (args.size() < 4 || args.size() > 6) {
            return wrongArity("zrange");
        }
        long start = parseLong(args.get(2), "start");
        long stop = parseLong(args.get(3), "stop");

        boolean withScores = false;
        boolean rev = false;
        for (int i = 4; i < args.size(); i++) {
            byte[] opt = args.get(i);
            if (asciiEqualsIgnoreCase(opt, "WITHSCORES")) {
                withScores = true;
                continue;
            }
            if (asciiEqualsIgnoreCase(opt, "REV")) {
                rev = true;
                continue;
            }
            return RespError.of("ERR syntax error");
        }

        return toBulkStringArray(rev
                ? db.zrevrange(args.get(1), start, stop, withScores)
                : db.zrange(args.get(1), start, stop, withScores));
    }

    private RespObject zrevrange(List<byte[]> args) {
        if (args.size() != 4 && args.size() != 5) {
            return wrongArity("zrevrange");
        }
        long start = parseLong(args.get(2), "start");
        long stop = parseLong(args.get(3), "stop");

        boolean withScores = false;
        if (args.size() == 5) {
            if (!asciiEqualsIgnoreCase(args.get(4), "WITHSCORES")) {
                return RespError.of("ERR syntax error");
            }
            withScores = true;
        }

        return toBulkStringArray(db.zrevrange(args.get(1), start, stop, withScores));
    }

    private RespObject zrangebyscore(List<byte[]> args) {
        if (args.size() < 4) {
            return wrongArity("zrangebyscore");
        }

        ScoreBound min = parseScoreBound(args.get(2));
        ScoreBound max = parseScoreBound(args.get(3));

        boolean withScores = false;
        long offset = 0;
        long count = Long.MAX_VALUE;

        int i = 4;
        while (i < args.size()) {
            byte[] opt = args.get(i);
            if (asciiEqualsIgnoreCase(opt, "WITHSCORES")) {
                withScores = true;
                i++;
                continue;
            }
            if (asciiEqualsIgnoreCase(opt, "LIMIT")) {
                if (i + 2 >= args.size()) {
                    return RespError.of("ERR syntax error");
                }
                offset = parseNonNegativeLong(args.get(i + 1), "offset");
                count = parseNonNegativeLong(args.get(i + 2), "count");
                i += 3;
                continue;
            }
            return RespError.of("ERR syntax error");
        }

        return toBulkStringArray(db.zrangeByScore(args.get(1), min.value, min.exclusive, max.value, max.exclusive, withScores, offset, count));
    }

    private RespObject zremrangebyscore(List<byte[]> args) {
        if (args.size() != 4) {
            return wrongArity("zremrangebyscore");
        }

        ScoreBound min = parseScoreBound(args.get(2));
        ScoreBound max = parseScoreBound(args.get(3));
        return RespInteger.of(db.zremrangeByScore(args.get(1), min.value, min.exclusive, max.value, max.exclusive));
    }

    private RespObject zrevrangebyscore(List<byte[]> args) {
        if (args.size() < 4) {
            return wrongArity("zrevrangebyscore");
        }

        ScoreBound max = parseScoreBound(args.get(2));
        ScoreBound min = parseScoreBound(args.get(3));

        boolean withScores = false;
        long offset = 0;
        long count = Long.MAX_VALUE;

        int i = 4;
        while (i < args.size()) {
            byte[] opt = args.get(i);
            if (asciiEqualsIgnoreCase(opt, "WITHSCORES")) {
                withScores = true;
                i++;
                continue;
            }
            if (asciiEqualsIgnoreCase(opt, "LIMIT")) {
                if (i + 2 >= args.size()) {
                    return RespError.of("ERR syntax error");
                }
                offset = parseNonNegativeLong(args.get(i + 1), "offset");
                count = parseNonNegativeLong(args.get(i + 2), "count");
                i += 3;
                continue;
            }
            return RespError.of("ERR syntax error");
        }

        return toBulkStringArray(db.zrevrangeByScore(
                args.get(1),
                min.value, min.exclusive,
                max.value, max.exclusive,
                withScores,
                offset,
                count
        ));
    }

    private RespObject zremrangebyrank(List<byte[]> args) {
        if (args.size() != 4) {
            return wrongArity("zremrangebyrank");
        }
        long start = parseLong(args.get(2), "start");
        long stop = parseLong(args.get(3), "stop");
        return RespInteger.of(db.zremrangeByRank(args.get(1), start, stop));
    }

    private RespObject zrem(List<byte[]> args) {
        if (args.size() < 3) {
            return wrongArity("zrem");
        }
        return RespInteger.of(db.zrem(args.get(1), args.subList(2, args.size())));
    }

    private static RespArray toBulkStringArray(List<byte[]> values) {
        if (values == null) {
            return RespArray.nullArray();
        }
        if (values.isEmpty()) {
            return RespArray.empty();
        }
        List<RespObject> out = new ArrayList<>(values.size());
        for (byte[] v : values) {
            out.add(RespBulkString.ofBytes(v));
        }
        return RespArray.of(out);
    }

    private static RespError wrongArity(String cmdLower) {
        return RespError.of("ERR wrong number of arguments for '" + cmdLower + "' command");
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

    private static String utf8(byte[] s) {
        return s == null ? null : new String(s, StandardCharsets.UTF_8);
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
}
