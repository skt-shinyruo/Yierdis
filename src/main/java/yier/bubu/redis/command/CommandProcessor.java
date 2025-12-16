package yier.bubu.redis.command;

import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.ValueType;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public final class CommandProcessor {
    private final YierdisDb db;

    public CommandProcessor(YierdisDb db) {
        this.db = db;
    }

    public RespObject execute(List<String> args) {
        String cmd = upper(args.get(0));
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
                case "SMEMBERS":
                    return smembers(args);
                case "SISMEMBER":
                    return sismember(args);
                case "SCARD":
                    return scard(args);

                default:
                    return RespError.of("ERR unknown command '" + args.get(0) + "'");
            }
        } catch (YierdisDb.WrongTypeException e) {
            return RespError.of(e.getMessage());
        } catch (YierdisDb.YierdisCommandException e) {
            return RespError.of(e.getMessage());
        } catch (IllegalArgumentException e) {
            return RespError.of("ERR " + e.getMessage());
        }
    }

    private RespObject ping(List<String> args) {
        if (args.size() == 1) {
            return RespSimpleString.of("PONG");
        }
        if (args.size() == 2) {
            return RespBulkString.ofString(args.get(1));
        }
        return wrongArity("ping");
    }

    private RespObject echo(List<String> args) {
        if (args.size() != 2) {
            return wrongArity("echo");
        }
        return RespBulkString.ofString(args.get(1));
    }

    private RespObject hello(List<String> args) {
        // Minimal RESP2-friendly HELLO implementation.
        // Redis returns a map in RESP3 and an array of alternating keys/values in RESP2.
        String version = args.size() >= 2 ? args.get(1) : "2";
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

    private RespObject select(List<String> args) {
        if (args.size() != 2) {
            return wrongArity("select");
        }
        if ("0".equals(args.get(1))) {
            return RespSimpleString.of("OK");
        }
        return RespError.of("ERR only DB 0 is supported");
    }

    private RespObject type(List<String> args) {
        if (args.size() != 2) {
            return wrongArity("type");
        }
        ValueType t = db.typeOf(args.get(1));
        if (t == null) {
            return RespSimpleString.of("none");
        }
        return RespSimpleString.of(t.name().toLowerCase());
    }

    private RespObject keys(List<String> args) {
        if (args.size() != 2) {
            return wrongArity("keys");
        }
        Set<String> keys = db.keys(args.get(1));
        List<RespObject> out = new ArrayList<>(keys.size());
        for (String k : keys) {
            out.add(RespBulkString.ofString(k));
        }
        return RespArray.of(out);
    }

    private RespObject del(List<String> args) {
        if (args.size() < 2) {
            return wrongArity("del");
        }
        long removed = db.del(args.subList(1, args.size()));
        return RespInteger.of(removed);
    }

    private RespObject exists(List<String> args) {
        if (args.size() < 2) {
            return wrongArity("exists");
        }
        long count = db.exists(args.subList(1, args.size()));
        return RespInteger.of(count);
    }

    private RespObject set(List<String> args) {
        if (args.size() < 3) {
            return wrongArity("set");
        }

        String key = args.get(1);
        String value = args.get(2);

        YierdisDb.SetMode mode = YierdisDb.SetMode.NORMAL;
        YierdisDb.ExpireOption expire = null;

        // SET key value [EX seconds|PX milliseconds] [NX|XX]
        for (int i = 3; i < args.size(); i++) {
            String opt = upper(args.get(i));
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

    private RespObject get(List<String> args) {
        if (args.size() != 2) {
            return wrongArity("get");
        }
        String value = db.getString(args.get(1));
        return value == null ? RespBulkString.nullString() : RespBulkString.ofString(value);
    }

    private RespObject strlen(List<String> args) {
        if (args.size() != 2) {
            return wrongArity("strlen");
        }
        return RespInteger.of(db.strlen(args.get(1)));
    }

    private RespObject append(List<String> args) {
        if (args.size() != 3) {
            return wrongArity("append");
        }
        int len = db.append(args.get(1), args.get(2));
        return RespInteger.of(len);
    }

    private RespObject incrBy(List<String> args, long delta) {
        if (args.size() != 2) {
            return wrongArity(delta > 0 ? "incr" : "decr");
        }
        long v = db.incrBy(args.get(1), delta);
        return RespInteger.of(v);
    }

    private RespObject expire(List<String> args) {
        if (args.size() != 3) {
            return wrongArity("expire");
        }
        long seconds = parseLong(args.get(2), "seconds");
        return RespInteger.of(db.expire(args.get(1), seconds) ? 1 : 0);
    }

    private RespObject ttl(List<String> args) {
        if (args.size() != 2) {
            return wrongArity("ttl");
        }
        return RespInteger.of(db.ttlSeconds(args.get(1)));
    }

    private RespObject lpush(List<String> args) {
        if (args.size() < 3) {
            return wrongArity("lpush");
        }
        int len = db.lpush(args.get(1), args.subList(2, args.size()));
        return RespInteger.of(len);
    }

    private RespObject rpush(List<String> args) {
        if (args.size() < 3) {
            return wrongArity("rpush");
        }
        int len = db.rpush(args.get(1), args.subList(2, args.size()));
        return RespInteger.of(len);
    }

    private RespObject lrange(List<String> args) {
        if (args.size() != 4) {
            return wrongArity("lrange");
        }
        int start = (int) parseLong(args.get(2), "start");
        int stop = (int) parseLong(args.get(3), "stop");
        List<String> values = db.lrange(args.get(1), start, stop);
        return toBulkStringArray(values);
    }

    private RespObject lpop(List<String> args) {
        if (args.size() != 2 && args.size() != 3) {
            return wrongArity("lpop");
        }
        int count = 1;
        boolean hasCount = args.size() == 3;
        if (hasCount) {
            count = (int) parseLong(args.get(2), "count");
        }
        List<String> popped = db.lpop(args.get(1), count);
        return popResponse(popped, hasCount);
    }

    private RespObject rpop(List<String> args) {
        if (args.size() != 2 && args.size() != 3) {
            return wrongArity("rpop");
        }
        int count = 1;
        boolean hasCount = args.size() == 3;
        if (hasCount) {
            count = (int) parseLong(args.get(2), "count");
        }
        List<String> popped = db.rpop(args.get(1), count);
        return popResponse(popped, hasCount);
    }

    private static RespObject popResponse(List<String> popped, boolean hasCount) {
        if (!hasCount) {
            if (popped.isEmpty()) {
                return RespBulkString.nullString();
            }
            return RespBulkString.ofString(popped.get(0));
        }
        return toBulkStringArray(popped);
    }

    private RespObject hset(List<String> args) {
        if (args.size() < 4) {
            return wrongArity("hset");
        }
        int added = db.hset(args.get(1), args.subList(2, args.size()));
        return RespInteger.of(added);
    }

    private RespObject hget(List<String> args) {
        if (args.size() != 3) {
            return wrongArity("hget");
        }
        String v = db.hget(args.get(1), args.get(2));
        return v == null ? RespBulkString.nullString() : RespBulkString.ofString(v);
    }

    private RespObject hgetall(List<String> args) {
        if (args.size() != 2) {
            return wrongArity("hgetall");
        }
        return toBulkStringArray(db.hgetall(args.get(1)));
    }

    private RespObject hlen(List<String> args) {
        if (args.size() != 2) {
            return wrongArity("hlen");
        }
        return RespInteger.of(db.hlen(args.get(1)));
    }

    private RespObject hdel(List<String> args) {
        if (args.size() < 3) {
            return wrongArity("hdel");
        }
        return RespInteger.of(db.hdel(args.get(1), args.subList(2, args.size())));
    }

    private RespObject sadd(List<String> args) {
        if (args.size() < 3) {
            return wrongArity("sadd");
        }
        return RespInteger.of(db.sadd(args.get(1), args.subList(2, args.size())));
    }

    private RespObject smembers(List<String> args) {
        if (args.size() != 2) {
            return wrongArity("smembers");
        }
        return toBulkStringArray(db.smembers(args.get(1)));
    }

    private RespObject sismember(List<String> args) {
        if (args.size() != 3) {
            return wrongArity("sismember");
        }
        return RespInteger.of(db.sismember(args.get(1), args.get(2)) ? 1 : 0);
    }

    private RespObject scard(List<String> args) {
        if (args.size() != 2) {
            return wrongArity("scard");
        }
        return RespInteger.of(db.scard(args.get(1)));
    }

    private static RespArray toBulkStringArray(List<String> values) {
        if (values == null) {
            return RespArray.nullArray();
        }
        if (values.isEmpty()) {
            return RespArray.empty();
        }
        List<RespObject> out = new ArrayList<>(values.size());
        for (String v : values) {
            out.add(RespBulkString.ofString(v));
        }
        return RespArray.of(out);
    }

    private static RespError wrongArity(String cmdLower) {
        return RespError.of("ERR wrong number of arguments for '" + cmdLower + "' command");
    }

    private static String upper(String s) {
        return s == null ? null : s.toUpperCase();
    }

    private static long parseLong(String s, String label) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("value is not an integer or out of range: " + label);
        }
    }
}
