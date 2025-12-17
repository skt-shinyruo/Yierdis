package yier.bubu.redis.command;

import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.ValueType;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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
        String cmd = upperAscii(args.get(0));
        try {
            switch (cmd) {
                case "PING":
                    return ping(args);
                case "ECHO":
                    return echo(args);
                case "HELLO":
                    return hello(args);
                case "COMMAND":
                    return RespArray.empty();
                case "SELECT":
                    return select(args);
                case "FLUSHDB":
                    db.flushDb();
                    return RespSimpleString.of("OK");
                case "TYPE":
                    return type(args);
                case "KEYS":
                    return keys(args);
                case "DEL":
                    return del(args);
                case "EXISTS":
                    return exists(args);
                case "SET":
                    return set(args);
                case "GET":
                    return get(args);
                case "STRLEN":
                    return strlen(args);
                case "APPEND":
                    return append(args);
                case "INCR":
                    return incrBy(args, 1);
                case "DECR":
                    return incrBy(args, -1);
                case "EXPIRE":
                    return expire(args);
                case "TTL":
                    return ttl(args);

                case "LPUSH":
                    return lpush(args);
                case "RPUSH":
                    return rpush(args);
                case "LRANGE":
                    return lrange(args);
                case "LPOP":
                    return lpop(args);
                case "RPOP":
                    return rpop(args);

                case "HSET":
                    return hset(args);
                case "HGET":
                    return hget(args);
                case "HGETALL":
                    return hgetall(args);
                case "HLEN":
                    return hlen(args);
                case "HDEL":
                    return hdel(args);

                case "SADD":
                    return sadd(args);
                case "SREM":
                    return srem(args);
                case "SMEMBERS":
                    return smembers(args);
                case "SISMEMBER":
                    return sismember(args);
                case "SCARD":
                    return scard(args);

                case "ZADD":
                    return zadd(args);
                case "ZRANGE":
                    return zrange(args);
                case "ZREVRANGE":
                    return zrevrange(args);
                case "ZRANGEBYSCORE":
                    return zrangebyscore(args);
                case "ZREVRANGEBYSCORE":
                    return zrevrangebyscore(args);
                case "ZREMRANGEBYSCORE":
                    return zremrangebyscore(args);
                case "ZREMRANGEBYRANK":
                    return zremrangebyrank(args);
                case "ZREM":
                    return zrem(args);

                default:
                    return RespError.of("ERR unknown command '" + utf8(args.get(0)) + "'");
            }
        } catch (YierdisDb.WrongTypeException e) {
            return RespError.of(e.getMessage());
        } catch (YierdisDb.YierdisCommandException e) {
            return RespError.of(e.getMessage());
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
        if ("0".equals(ascii(args.get(1)))) {
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
        return RespSimpleString.of(t.name().toLowerCase());
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
            String opt = upperAscii(args.get(i));
            if ("NX".equals(opt)) {
                mode = YierdisDb.SetMode.NX;
                continue;
            }
            if ("XX".equals(opt)) {
                mode = YierdisDb.SetMode.XX;
                continue;
            }
            if ("EX".equals(opt) && i + 1 < args.size()) {
                long seconds = parseLong(args.get(++i), "seconds");
                expire = new YierdisDb.ExpireOption(TimeUnit.SECONDS, seconds);
                continue;
            }
            if ("PX".equals(opt) && i + 1 < args.size()) {
                long millis = parseLong(args.get(++i), "milliseconds");
                expire = new YierdisDb.ExpireOption(TimeUnit.MILLISECONDS, millis);
                continue;
            }
            return RespError.of("ERR syntax error");
        }

        boolean ok = db.setString(key, value, mode, expire);
        if (!ok) {
            return RespBulkString.nullString();
        }
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
        int len = db.append(args.get(1), args.get(2));
        return RespInteger.of(len);
    }

    private RespObject incrBy(List<byte[]> args, long delta) {
        if (args.size() != 2) {
            return wrongArity(delta > 0 ? "incr" : "decr");
        }
        long v = db.incrBy(args.get(1), delta);
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
        int len = db.lpush(args.get(1), args.subList(2, args.size()));
        return RespInteger.of(len);
    }

    private RespObject rpush(List<byte[]> args) {
        if (args.size() < 3) {
            return wrongArity("rpush");
        }
        int len = db.rpush(args.get(1), args.subList(2, args.size()));
        return RespInteger.of(len);
    }

    private RespObject lrange(List<byte[]> args) {
        if (args.size() != 4) {
            return wrongArity("lrange");
        }
        int start = (int) parseLong(args.get(2), "start");
        int stop = (int) parseLong(args.get(3), "stop");
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
            count = (int) parseLong(args.get(2), "count");
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
            count = (int) parseLong(args.get(2), "count");
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
        int added = db.hset(args.get(1), args.subList(2, args.size()));
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
        return RespInteger.of(db.sadd(args.get(1), args.subList(2, args.size())));
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
        int added = db.zadd(args.get(1), args.subList(2, args.size()));
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
            String opt = upperAscii(args.get(i));
            if ("WITHSCORES".equals(opt)) {
                withScores = true;
                continue;
            }
            if ("REV".equals(opt)) {
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
            if (!"WITHSCORES".equalsIgnoreCase(ascii(args.get(4)))) {
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
            String opt = upperAscii(args.get(i));
            if ("WITHSCORES".equals(opt)) {
                withScores = true;
                i++;
                continue;
            }
            if ("LIMIT".equals(opt)) {
                if (i + 2 >= args.size()) {
                    return RespError.of("ERR syntax error");
                }
                offset = parseLong(args.get(i + 1), "offset");
                count = parseLong(args.get(i + 2), "count");
                if (offset < 0 || count < 0) {
                    return RespError.of("ERR syntax error");
                }
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
            String opt = upperAscii(args.get(i));
            if ("WITHSCORES".equals(opt)) {
                withScores = true;
                i++;
                continue;
            }
            if ("LIMIT".equals(opt)) {
                if (i + 2 >= args.size()) {
                    return RespError.of("ERR syntax error");
                }
                offset = parseLong(args.get(i + 1), "offset");
                count = parseLong(args.get(i + 2), "count");
                if (offset < 0 || count < 0) {
                    return RespError.of("ERR syntax error");
                }
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

    private static String upperAscii(byte[] s) {
        return s == null ? null : new String(s, StandardCharsets.US_ASCII).toUpperCase();
    }

    private static String ascii(byte[] s) {
        return s == null ? null : new String(s, StandardCharsets.US_ASCII);
    }

    private static String utf8(byte[] s) {
        return s == null ? null : new String(s, StandardCharsets.UTF_8);
    }

    private static long parseLong(byte[] s, String label) {
        try {
            return Long.parseLong(ascii(s));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("value is not an integer or out of range: " + label);
        }
    }

    private static ScoreBound parseScoreBound(byte[] raw) {
        String s = ascii(raw);
        if (s == null || s.isEmpty()) {
            throw new YierdisDb.YierdisCommandException("ERR min or max is not a float");
        }
        s = s.trim();
        boolean exclusive = false;
        if (s.startsWith("(")) {
            exclusive = true;
            s = s.substring(1);
        } else if (s.startsWith("[")) {
            // Accept bracket-inclusive to be tolerant with newer Redis range syntaxes.
            s = s.substring(1);
        }

        if ("-inf".equalsIgnoreCase(s)) {
            return new ScoreBound(Double.NEGATIVE_INFINITY, exclusive);
        }
        if ("+inf".equalsIgnoreCase(s) || "inf".equalsIgnoreCase(s)) {
            return new ScoreBound(Double.POSITIVE_INFINITY, exclusive);
        }

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
