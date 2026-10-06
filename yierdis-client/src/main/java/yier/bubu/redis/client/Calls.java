package yier.bubu.redis.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 普通连接、管道和事务共用的参数顺序。三处只负责各自怎么收回复。
 */
final class Calls {
    private Calls() {
    }

    static Call<String> ping() {
        return text("PING");
    }

    static Call<String> ping(String message) {
        return text("PING", message);
    }

    static Call<String> echo(String message) {
        return text("ECHO", message);
    }

    static Call<List<CommandInfo>> commandList() {
        return Call.of(Replies::commandInfos, "COMMAND");
    }

    static Call<Long> commandCount() {
        return integer("COMMAND", "COUNT");
    }

    static Call<List<CommandInfo>> commandInfo(String... names) {
        ArrayList<String> args = words("COMMAND", "INFO");
        Collections.addAll(args, names);
        return Call.of(Replies::commandInfos, args);
    }

    static Call<String> select(int index) {
        return text("SELECT", Integer.toString(index));
    }

    static Call<String> quit() {
        return text("QUIT");
    }

    static Call<String> clientSetname(String name) {
        return text("CLIENT", "SETNAME", name);
    }

    static Call<String> clientGetname() {
        return textOrNull("CLIENT", "GETNAME");
    }

    static Call<String> clientSetinfo(String attribute, String value) {
        return text("CLIENT", "SETINFO", attribute, value);
    }

    static Call<Object> auth(String password) {
        return Call.raw("AUTH", password);
    }

    static Call<Object> auth(String username, String password) {
        return Call.raw("AUTH", username, password);
    }

    static Call<String> flushdb() {
        return text("FLUSHDB");
    }

    static Call<String> flushdb(FlushMode mode) {
        Objects.requireNonNull(mode, "mode");
        return text("FLUSHDB", mode.name());
    }

    static Call<String> info() {
        return text("INFO");
    }

    static Call<String> info(String... sections) {
        ArrayList<String> args = words("INFO");
        Collections.addAll(args, sections);
        return text(args);
    }

    static Call<Map<String, Object>> infoHealth() {
        return Call.of(Replies::fieldMap, "INFO", "health");
    }

    static Call<Map<String, Object>> infoYierdis() {
        return Call.of(Replies::fieldMap, "INFO", "yierdis");
    }

    static Call<Map<String, Object>> stats() {
        return Call.of(Replies::fieldMap, "STATS");
    }

    static Call<String> ydreconcile() {
        return text("YDRECONCILE");
    }

    static Call<String> set(String key, String value) {
        return textOrNull("SET", key, value);
    }

    static Call<String> set(String key, String value, SetOptions options) {
        Objects.requireNonNull(options, "options");
        ArrayList<String> args = words("SET", key, value);
        args.addAll(options.tokens());
        return textOrNull(args);
    }

    static Call<String> setGet(String key, String value) {
        return textOrNull("SET", key, value, "GET");
    }

    static Call<String> setGet(String key, String value, SetGetOptions options) {
        Objects.requireNonNull(options, "options");
        ArrayList<String> args = words("SET", key, value);
        args.addAll(options.tokens());
        return textOrNull(args);
    }

    static Call<String> get(String key) {
        return textOrNull("GET", key);
    }

    static Call<Long> strlen(String key) {
        return integer("STRLEN", key);
    }

    static Call<Long> append(String key, String value) {
        return integer("APPEND", key, value);
    }

    static Call<Long> setbit(String key, long offset, long value) {
        return integer("SETBIT", key, Long.toString(offset), Long.toString(value));
    }

    static Call<Long> getbit(String key, long offset) {
        return integer("GETBIT", key, Long.toString(offset));
    }

    static Call<Long> bitcount(String key) {
        return integer("BITCOUNT", key);
    }

    static Call<Long> bitcount(String key, long start, long end) {
        return integer("BITCOUNT", key, Long.toString(start), Long.toString(end));
    }

    static Call<Long> incr(String key) {
        return integer("INCR", key);
    }

    static Call<Long> decr(String key) {
        return integer("DECR", key);
    }

    static Call<Long> hset(String key, String field, String value, String... more) {
        ArrayList<String> args = words("HSET", key, field, value);
        Collections.addAll(args, more);
        return integer(args);
    }

    static Call<String> hget(String key, String field) {
        return textOrNull("HGET", key, field);
    }

    static Call<Map<String, String>> hgetall(String key) {
        return Call.of(Replies::stringMap, "HGETALL", key);
    }

    static Call<Long> hlen(String key) {
        return integer("HLEN", key);
    }

    static Call<Long> hdel(String key, String field, String... more) {
        ArrayList<String> args = words("HDEL", key, field);
        Collections.addAll(args, more);
        return integer(args);
    }

    static Call<HashScan> hscan(String key, String cursor) {
        return Call.of(Replies::hashScan, "HSCAN", key, cursor);
    }

    static Call<HashScan> hscan(String key, String cursor, ScanOptions options) {
        return Call.of(Replies::hashScan, scanArgs("HSCAN", key, cursor, options, false));
    }

    static Call<HashFieldScan> hscanNoValues(String key, String cursor) {
        return Call.of(Replies::hashFieldScan, "HSCAN", key, cursor, "NOVALUES");
    }

    static Call<HashFieldScan> hscanNoValues(String key, String cursor, ScanOptions options) {
        return Call.of(Replies::hashFieldScan, scanArgs("HSCAN", key, cursor, options, true));
    }

    static Call<Long> lpush(String key, String value, String... more) {
        return variadicInteger("LPUSH", key, value, more);
    }

    static Call<Long> rpush(String key, String value, String... more) {
        return variadicInteger("RPUSH", key, value, more);
    }

    static Call<List<String>> lrange(String key, long start, long stop) {
        return strings("LRANGE", key, Long.toString(start), Long.toString(stop));
    }

    static Call<String> lpop(String key) {
        return textOrNull("LPOP", key);
    }

    static Call<List<String>> lpop(String key, long count) {
        return stringsOrNull("LPOP", key, Long.toString(count));
    }

    static Call<String> rpop(String key) {
        return textOrNull("RPOP", key);
    }

    static Call<List<String>> rpop(String key, long count) {
        return stringsOrNull("RPOP", key, Long.toString(count));
    }

    static Call<Long> sadd(String key, String member, String... more) {
        return variadicInteger("SADD", key, member, more);
    }

    static Call<Long> srem(String key, String member, String... more) {
        return variadicInteger("SREM", key, member, more);
    }

    static Call<Set<String>> smembers(String key) {
        return Call.of(Replies::orderedSet, "SMEMBERS", key);
    }

    static Call<Long> sismember(String key, String member) {
        return integer("SISMEMBER", key, member);
    }

    static Call<Long> scard(String key) {
        return integer("SCARD", key);
    }

    static Call<SetScan> sscan(String key, String cursor) {
        return Call.of(Replies::setScan, "SSCAN", key, cursor);
    }

    static Call<SetScan> sscan(String key, String cursor, ScanOptions options) {
        return Call.of(Replies::setScan, scanArgs("SSCAN", key, cursor, options, false));
    }

    static Call<Long> zadd(String key, String score, String member, String... more) {
        return zaddPairs(key, List.of(), score, member, more);
    }

    static Call<Long> zadd(String key, ZAddOptions options, String score, String member, String... more) {
        Objects.requireNonNull(options, "options");
        return zaddPairs(key, options.tokens(), score, member, more);
    }

    static Call<String> zaddIncr(String key, String score, String member) {
        return zaddIncrPairs(key, List.of("INCR"), score, member);
    }

    static Call<String> zaddIncr(String key, ZAddIncrOptions options, String score, String member) {
        Objects.requireNonNull(options, "options");
        return zaddIncrPairs(key, options.tokens(), score, member);
    }

    static Call<List<String>> zrange(String key, long start, long stop) {
        return strings(zrangeArgs(key, start, stop, false, null));
    }

    static Call<List<String>> zrange(String key, long start, long stop, ZRangeOptions options) {
        Objects.requireNonNull(options, "options");
        return strings(zrangeArgs(key, start, stop, false, options));
    }

    static Call<List<ScoredMember>> zrangeWithScores(String key, long start, long stop) {
        return scored(zrangeArgs(key, start, stop, true, null));
    }

    static Call<List<ScoredMember>> zrangeWithScores(String key, long start, long stop, ZRangeOptions options) {
        return scored(zrangeArgs(key, start, stop, true, options));
    }

    static Call<List<String>> zrevrange(String key, long start, long stop) {
        return strings("ZREVRANGE", key, Long.toString(start), Long.toString(stop));
    }

    static Call<List<ScoredMember>> zrevrangeWithScores(String key, long start, long stop) {
        return scored("ZREVRANGE", key, Long.toString(start), Long.toString(stop), "WITHSCORES");
    }

    static Call<List<String>> zrangeByScore(String key, String min, String max) {
        return strings(scoreRangeArgs("ZRANGEBYSCORE", key, min, max, false, null));
    }

    static Call<List<String>> zrangeByScore(String key, String min, String max, ScoreRangeOptions options) {
        Objects.requireNonNull(options, "options");
        return strings(scoreRangeArgs("ZRANGEBYSCORE", key, min, max, false, options));
    }

    static Call<List<ScoredMember>> zrangeByScoreWithScores(String key, String min, String max) {
        return scored(scoreRangeArgs("ZRANGEBYSCORE", key, min, max, true, null));
    }

    static Call<List<ScoredMember>> zrangeByScoreWithScores(
            String key,
            String min,
            String max,
            ScoreRangeOptions options
    ) {
        Objects.requireNonNull(options, "options");
        return scored(scoreRangeArgs("ZRANGEBYSCORE", key, min, max, true, options));
    }

    static Call<List<String>> zrevrangeByScore(String key, String max, String min) {
        return strings(scoreRangeArgs("ZREVRANGEBYSCORE", key, max, min, false, null));
    }

    static Call<List<String>> zrevrangeByScore(String key, String max, String min, ScoreRangeOptions options) {
        Objects.requireNonNull(options, "options");
        return strings(scoreRangeArgs("ZREVRANGEBYSCORE", key, max, min, false, options));
    }

    static Call<List<ScoredMember>> zrevrangeByScoreWithScores(String key, String max, String min) {
        return scored(scoreRangeArgs("ZREVRANGEBYSCORE", key, max, min, true, null));
    }

    static Call<List<ScoredMember>> zrevrangeByScoreWithScores(
            String key,
            String max,
            String min,
            ScoreRangeOptions options
    ) {
        Objects.requireNonNull(options, "options");
        return scored(scoreRangeArgs("ZREVRANGEBYSCORE", key, max, min, true, options));
    }

    static Call<Long> zremrangeByScore(String key, String min, String max) {
        return integer("ZREMRANGEBYSCORE", key, min, max);
    }

    static Call<Long> zremrangeByRank(String key, long start, long stop) {
        return integer("ZREMRANGEBYRANK", key, Long.toString(start), Long.toString(stop));
    }

    static Call<Long> zrem(String key, String member, String... more) {
        return variadicInteger("ZREM", key, member, more);
    }

    static Call<ZScan> zscan(String key, String cursor) {
        return Call.of(Replies::zscan, "ZSCAN", key, cursor);
    }

    static Call<ZScan> zscan(String key, String cursor, ScanOptions options) {
        return Call.of(Replies::zscan, scanArgs("ZSCAN", key, cursor, options, false));
    }

    static Call<String> type(String key) {
        return text("TYPE", key);
    }

    static Call<Long> memoryUsage(String key) {
        return integerOrNull("MEMORY", "USAGE", key);
    }

    static Call<Long> memoryUsage(String key, MemoryUsageOptions options) {
        Objects.requireNonNull(options, "options");
        ArrayList<String> args = words("MEMORY", "USAGE", key);
        args.addAll(options.tokens());
        return integerOrNull(args);
    }

    static Call<Map<String, Long>> memoryStats() {
        return Call.of(Replies::longMap, "MEMORY", "STATS");
    }

    static Call<String> objectEncoding(String key) {
        return textOrNull("OBJECT", "ENCODING", key);
    }

    static Call<List<String>> keys(String pattern) {
        return strings("KEYS", pattern);
    }

    static Call<KeyScan> scan(String cursor) {
        return Call.of(Replies::keyScan, "SCAN", cursor);
    }

    static Call<KeyScan> scan(String cursor, ScanOptions options) {
        Objects.requireNonNull(options, "options");
        ArrayList<String> args = words("SCAN", cursor);
        args.addAll(options.tokens());
        return Call.of(Replies::keyScan, args);
    }

    static Call<Long> del(String... keys) {
        return variadicInteger("DEL", keys);
    }

    static Call<Long> exists(String... keys) {
        return variadicInteger("EXISTS", keys);
    }

    static Call<Long> expire(String key, long seconds) {
        return expireCommand("EXPIRE", key, seconds, null);
    }

    static Call<Long> expire(String key, long seconds, ExpireOptions options) {
        Objects.requireNonNull(options, "options");
        return expireCommand("EXPIRE", key, seconds, options);
    }

    static Call<Long> pexpire(String key, long milliseconds) {
        return expireCommand("PEXPIRE", key, milliseconds, null);
    }

    static Call<Long> pexpire(String key, long milliseconds, ExpireOptions options) {
        Objects.requireNonNull(options, "options");
        return expireCommand("PEXPIRE", key, milliseconds, options);
    }

    static Call<Long> expireat(String key, long unixSeconds) {
        return expireCommand("EXPIREAT", key, unixSeconds, null);
    }

    static Call<Long> expireat(String key, long unixSeconds, ExpireOptions options) {
        Objects.requireNonNull(options, "options");
        return expireCommand("EXPIREAT", key, unixSeconds, options);
    }

    static Call<Long> pexpireat(String key, long unixMilliseconds) {
        return expireCommand("PEXPIREAT", key, unixMilliseconds, null);
    }

    static Call<Long> pexpireat(String key, long unixMilliseconds, ExpireOptions options) {
        Objects.requireNonNull(options, "options");
        return expireCommand("PEXPIREAT", key, unixMilliseconds, options);
    }

    static Call<Long> persist(String key) {
        return integer("PERSIST", key);
    }

    static Call<Long> ttl(String key) {
        return integer("TTL", key);
    }

    static Call<Long> pttl(String key) {
        return integer("PTTL", key);
    }

    static Call<Long> pfadd(String key, String... elements) {
        ArrayList<String> args = words("PFADD", key);
        Collections.addAll(args, elements);
        return integer(args);
    }

    static Call<Long> pfcount(String... keys) {
        return variadicInteger("PFCOUNT", keys);
    }

    static Call<String> pfmerge(String destination, String... sources) {
        ArrayList<String> args = words("PFMERGE", destination);
        Collections.addAll(args, sources);
        return text(args);
    }

    private static Call<Long> zaddPairs(
            String key,
            List<String> optionTokens,
            String score,
            String member,
            String... more
    ) {
        ArrayList<String> args = words("ZADD", key);
        args.addAll(optionTokens);
        args.add(score);
        args.add(member);
        Collections.addAll(args, more);
        return integer(args);
    }

    private static Call<String> zaddIncrPairs(
            String key,
            List<String> optionTokens,
            String score,
            String member
    ) {
        ArrayList<String> args = words("ZADD", key);
        args.addAll(optionTokens);
        args.add(score);
        args.add(member);
        return textOrNull(args);
    }

    private static ArrayList<String> zrangeArgs(
            String key,
            long start,
            long stop,
            boolean withScores,
            ZRangeOptions options
    ) {
        ArrayList<String> args = words("ZRANGE", key, Long.toString(start), Long.toString(stop));
        if (options != null) {
            args.addAll(options.tokens(withScores));
        } else if (withScores) {
            args.add("WITHSCORES");
        }
        return args;
    }

    private static ArrayList<String> scoreRangeArgs(
            String commandName,
            String key,
            String start,
            String end,
            boolean withScores,
            ScoreRangeOptions options
    ) {
        ArrayList<String> args = words(commandName, key, start, end);
        if (withScores) {
            args.add("WITHSCORES");
        }
        if (options != null) {
            args.addAll(options.tokens());
        }
        return args;
    }

    private static Call<Long> expireCommand(String commandName, String key, long value, ExpireOptions options) {
        ArrayList<String> args = words(commandName, key, Long.toString(value));
        if (options != null) {
            args.addAll(options.tokens());
        }
        return integer(args);
    }

    private static Call<Long> variadicInteger(String commandName, String... parts) {
        ArrayList<String> args = words(commandName);
        Collections.addAll(args, parts);
        return integer(args);
    }

    private static Call<Long> variadicInteger(String commandName, String key, String first, String... more) {
        ArrayList<String> args = words(commandName, key, first);
        Collections.addAll(args, more);
        return integer(args);
    }

    private static ArrayList<String> scanArgs(
            String commandName,
            String key,
            String cursor,
            ScanOptions options,
            boolean noValues
    ) {
        Objects.requireNonNull(options, "options");
        ArrayList<String> args = words(commandName, key, cursor);
        args.addAll(options.tokens());
        if (noValues) {
            args.add("NOVALUES");
        }
        return args;
    }

    private static Call<String> text(String... args) {
        return Call.of(Replies::text, args);
    }

    private static Call<String> text(List<String> args) {
        return Call.of(Replies::text, args.toArray(String[]::new));
    }

    private static Call<String> textOrNull(String... args) {
        return Call.of(Replies::textOrNull, args);
    }

    private static Call<String> textOrNull(List<String> args) {
        return Call.of(Replies::textOrNull, args.toArray(String[]::new));
    }

    private static Call<Long> integer(String... args) {
        return Call.of(Replies::integer, args);
    }

    private static Call<Long> integer(List<String> args) {
        return Call.of(Replies::integer, args.toArray(String[]::new));
    }

    private static Call<Long> integerOrNull(String... args) {
        return Call.of(Replies::integerOrNull, args);
    }

    private static Call<Long> integerOrNull(List<String> args) {
        return Call.of(Replies::integerOrNull, args.toArray(String[]::new));
    }

    private static Call<List<String>> strings(String... args) {
        return Call.of(Replies::strings, args);
    }

    private static Call<List<String>> strings(List<String> args) {
        return Call.of(Replies::strings, args.toArray(String[]::new));
    }

    private static Call<List<String>> stringsOrNull(String... args) {
        return Call.of(Replies::stringsOrNull, args);
    }

    private static Call<List<ScoredMember>> scored(String... args) {
        return Call.of(Replies::scoredMembers, args);
    }

    private static Call<List<ScoredMember>> scored(List<String> args) {
        return Call.of(Replies::scoredMembers, args.toArray(String[]::new));
    }

    private static ArrayList<String> words(String... args) {
        ArrayList<String> words = new ArrayList<>(args.length);
        Collections.addAll(words, args);
        return words;
    }
}
