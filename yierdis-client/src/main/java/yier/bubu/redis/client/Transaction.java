package yier.bubu.redis.client;

import yier.bubu.redis.client.command.ExpireOptions;
import yier.bubu.redis.client.command.FlushMode;
import yier.bubu.redis.client.command.MemoryUsageOptions;
import yier.bubu.redis.client.command.ScanOptions;
import yier.bubu.redis.client.command.ScoreRangeOptions;
import yier.bubu.redis.client.command.SetGetOptions;
import yier.bubu.redis.client.command.SetOptions;
import yier.bubu.redis.client.command.ZAddIncrOptions;
import yier.bubu.redis.client.command.ZAddOptions;
import yier.bubu.redis.client.command.ZRangeOptions;
import yier.bubu.redis.client.exception.DecodeException;
import yier.bubu.redis.client.exception.ServerException;
import yier.bubu.redis.client.internal.Call;
import yier.bubu.redis.client.internal.Calls;
import yier.bubu.redis.client.reply.CommandInfo;
import yier.bubu.redis.client.reply.HashFieldScan;
import yier.bubu.redis.client.reply.HashScan;
import yier.bubu.redis.client.reply.KeyScan;
import yier.bubu.redis.client.reply.ScoredMember;
import yier.bubu.redis.client.reply.SetScan;
import yier.bubu.redis.client.reply.ZScan;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * 同一条连接上的事务。{@code MULTI} 已在 {@link Connection#multi()} 里提交。
 * <p>
 * 类型化命令立刻提交。它的 {@link CompletableFuture} 在 {@code EXEC} 时完成，拿到的是执行结果，不是 {@code QUEUED}。
 * 原始 {@link #command(String...)} 同样在 {@code EXEC} 时完成；原始 {@code EXEC} 的列表里，单条错误仍是服务端原文。
 * {@link #exec()} 的列表里，单条错误是 {@link ServerException}，不让这次 {@code EXEC} 本身失败。
 * {@code EXEC} 自身失败则它的 Future 失败，事务结束。{@link #discard()} 成功才结束事务。
 */
public final class Transaction {
    private final Connection connection;
    private boolean finished;

    Transaction(Connection connection) {
        this.connection = connection;
    }

    /**
     * 写出后返回 Future。入队拒绝立刻失败这一条，不进入后来的 {@code EXEC} 结果列表。
     * 事务保持到 {@code EXEC} 或 {@code DISCARD}。{@code EXEC} 作废整笔排队时，已经入队的命令一起失败。
     * 嵌套 {@code MULTI} 在写出前拒绝。原始 {@code EXEC} 结束事务；原始 {@code DISCARD} 成功同样结束。
     */
    public CompletableFuture<Object> command(String... args) {
        return command(connection.commandTimeoutMillis(), args);
    }

    CompletableFuture<Object> command(long timeoutMillis, String[] args) {
        ensureActive();
        return connection.submitTransaction(timeoutMillis, Call.raw(args));
    }

    /**
     * 提交 {@code EXEC}。空队列也提交。单条执行错误留在列表里，这条 Future 仍完成。
     * {@code EXEC} 自身的错误，包括 {@code EXECABORT}，让这条 Future 失败并结束事务。
     */
    public CompletableFuture<List<Object>> exec() {
        ensureActive();
        return connection.submitTypedExec();
    }

    /**
     * 提交 {@code DISCARD}。{@code OK} 结束这个对象并让连接回到普通模式。
     * 服务端错误让这条 Future 失败，事务不结束。
     */
    public CompletableFuture<String> discard() {
        ensureActive();
        CompletableFuture<Object> raw = connection.submitTransaction(
                connection.commandTimeoutMillis(), Call.raw("DISCARD"));
        CompletableFuture<String> typed = new CompletableFuture<>();
        raw.whenComplete((value, error) -> {
            if (error != null) {
                typed.completeExceptionally(Connection.unwrap(error));
                return;
            }
            if (value instanceof String text) {
                typed.complete(text);
                return;
            }
            String actual = value == null ? "null" : value.getClass().getSimpleName();
            typed.completeExceptionally(new DecodeException("reply was " + actual + ", expected OK", null));
        });
        return typed;
    }

    void markFinished() {
        finished = true;
    }

    @SuppressWarnings("unchecked")
    private <T> CompletableFuture<T> queue(Call<T> call) {
        ensureActive();
        // 执行结果在 EXEC 时已经按这个 Call 解码，并完成同一次提交的 Future。
        return (CompletableFuture<T>) connection.submitTransaction(connection.commandTimeoutMillis(), call);
    }

    private void ensureActive() {
        if (connection.isClosed()) {
            throw new IllegalStateException("connection is closed");
        }
        if (finished || connection.currentTransaction() != this) {
            throw new IllegalStateException("transaction is finished");
        }
    }

    public CompletableFuture<String> ping() {
        return queue(Calls.ping());
    }

    public CompletableFuture<String> ping(String message) {
        return queue(Calls.ping(message));
    }

    public CompletableFuture<String> echo(String message) {
        return queue(Calls.echo(message));
    }

    public CompletableFuture<List<CommandInfo>> commandList() {
        return queue(Calls.commandList());
    }

    public CompletableFuture<Long> commandCount() {
        return queue(Calls.commandCount());
    }

    public CompletableFuture<List<CommandInfo>> commandInfo(String... names) {
        return queue(Calls.commandInfo(names));
    }

    public CompletableFuture<String> select(int index) {
        return queue(Calls.select(index));
    }

    public CompletableFuture<String> quit() {
        return queue(Calls.quit());
    }

    public CompletableFuture<String> clientSetname(String name) {
        return queue(Calls.clientSetname(name));
    }

    public CompletableFuture<String> clientGetname() {
        return queue(Calls.clientGetname());
    }

    public CompletableFuture<String> clientSetinfo(String attribute, String value) {
        return queue(Calls.clientSetinfo(attribute, value));
    }

    public CompletableFuture<Object> auth(String password) {
        return queue(Calls.auth(password));
    }

    public CompletableFuture<Object> auth(String username, String password) {
        return queue(Calls.auth(username, password));
    }

    public CompletableFuture<String> flushdb() {
        return queue(Calls.flushdb());
    }

    public CompletableFuture<String> flushdb(FlushMode mode) {
        return queue(Calls.flushdb(mode));
    }

    public CompletableFuture<String> info() {
        return queue(Calls.info());
    }

    public CompletableFuture<String> info(String... sections) {
        return queue(Calls.info(sections));
    }

    public CompletableFuture<Map<String, Object>> infoHealth() {
        return queue(Calls.infoHealth());
    }

    public CompletableFuture<Map<String, Object>> infoYierdis() {
        return queue(Calls.infoYierdis());
    }

    public CompletableFuture<Map<String, Object>> stats() {
        return queue(Calls.stats());
    }

    public CompletableFuture<String> ydreconcile() {
        return queue(Calls.ydreconcile());
    }

    public CompletableFuture<String> set(String key, String value) {
        return queue(Calls.set(key, value));
    }

    public CompletableFuture<String> set(String key, String value, SetOptions options) {
        return queue(Calls.set(key, value, options));
    }

    public CompletableFuture<String> setGet(String key, String value) {
        return queue(Calls.setGet(key, value));
    }

    public CompletableFuture<String> setGet(String key, String value, SetGetOptions options) {
        return queue(Calls.setGet(key, value, options));
    }

    public CompletableFuture<String> get(String key) {
        return queue(Calls.get(key));
    }

    public CompletableFuture<Long> strlen(String key) {
        return queue(Calls.strlen(key));
    }

    public CompletableFuture<Long> append(String key, String value) {
        return queue(Calls.append(key, value));
    }

    public CompletableFuture<Long> setbit(String key, long offset, long value) {
        return queue(Calls.setbit(key, offset, value));
    }

    public CompletableFuture<Long> getbit(String key, long offset) {
        return queue(Calls.getbit(key, offset));
    }

    public CompletableFuture<Long> bitcount(String key) {
        return queue(Calls.bitcount(key));
    }

    public CompletableFuture<Long> bitcount(String key, long start, long end) {
        return queue(Calls.bitcount(key, start, end));
    }

    public CompletableFuture<Long> incr(String key) {
        return queue(Calls.incr(key));
    }

    public CompletableFuture<Long> decr(String key) {
        return queue(Calls.decr(key));
    }

    public CompletableFuture<Long> hset(String key, String field, String value, String... more) {
        return queue(Calls.hset(key, field, value, more));
    }

    public CompletableFuture<String> hget(String key, String field) {
        return queue(Calls.hget(key, field));
    }

    public CompletableFuture<Map<String, String>> hgetall(String key) {
        return queue(Calls.hgetall(key));
    }

    public CompletableFuture<Long> hlen(String key) {
        return queue(Calls.hlen(key));
    }

    public CompletableFuture<Long> hdel(String key, String field, String... more) {
        return queue(Calls.hdel(key, field, more));
    }

    public CompletableFuture<HashScan> hscan(String key, String cursor) {
        return queue(Calls.hscan(key, cursor));
    }

    public CompletableFuture<HashScan> hscan(String key, String cursor, ScanOptions options) {
        return queue(Calls.hscan(key, cursor, options));
    }

    public CompletableFuture<HashFieldScan> hscanNoValues(String key, String cursor) {
        return queue(Calls.hscanNoValues(key, cursor));
    }

    public CompletableFuture<HashFieldScan> hscanNoValues(String key, String cursor, ScanOptions options) {
        return queue(Calls.hscanNoValues(key, cursor, options));
    }

    public CompletableFuture<Long> lpush(String key, String value, String... more) {
        return queue(Calls.lpush(key, value, more));
    }

    public CompletableFuture<Long> rpush(String key, String value, String... more) {
        return queue(Calls.rpush(key, value, more));
    }

    public CompletableFuture<List<String>> lrange(String key, long start, long stop) {
        return queue(Calls.lrange(key, start, stop));
    }

    public CompletableFuture<String> lpop(String key) {
        return queue(Calls.lpop(key));
    }

    public CompletableFuture<List<String>> lpop(String key, long count) {
        return queue(Calls.lpop(key, count));
    }

    public CompletableFuture<String> rpop(String key) {
        return queue(Calls.rpop(key));
    }

    public CompletableFuture<List<String>> rpop(String key, long count) {
        return queue(Calls.rpop(key, count));
    }

    public CompletableFuture<Long> sadd(String key, String member, String... more) {
        return queue(Calls.sadd(key, member, more));
    }

    public CompletableFuture<Long> srem(String key, String member, String... more) {
        return queue(Calls.srem(key, member, more));
    }

    public CompletableFuture<Set<String>> smembers(String key) {
        return queue(Calls.smembers(key));
    }

    public CompletableFuture<Long> sismember(String key, String member) {
        return queue(Calls.sismember(key, member));
    }

    public CompletableFuture<Long> scard(String key) {
        return queue(Calls.scard(key));
    }

    public CompletableFuture<SetScan> sscan(String key, String cursor) {
        return queue(Calls.sscan(key, cursor));
    }

    public CompletableFuture<SetScan> sscan(String key, String cursor, ScanOptions options) {
        return queue(Calls.sscan(key, cursor, options));
    }

    public CompletableFuture<Long> zadd(String key, String score, String member, String... more) {
        return queue(Calls.zadd(key, score, member, more));
    }

    public CompletableFuture<Long> zadd(String key, ZAddOptions options, String score, String member, String... more) {
        return queue(Calls.zadd(key, options, score, member, more));
    }

    public CompletableFuture<String> zaddIncr(String key, String score, String member) {
        return queue(Calls.zaddIncr(key, score, member));
    }

    public CompletableFuture<String> zaddIncr(String key, ZAddIncrOptions options, String score, String member) {
        return queue(Calls.zaddIncr(key, options, score, member));
    }

    public CompletableFuture<List<String>> zrange(String key, long start, long stop) {
        return queue(Calls.zrange(key, start, stop));
    }

    public CompletableFuture<List<String>> zrange(String key, long start, long stop, ZRangeOptions options) {
        return queue(Calls.zrange(key, start, stop, options));
    }

    public CompletableFuture<List<ScoredMember>> zrangeWithScores(String key, long start, long stop) {
        return queue(Calls.zrangeWithScores(key, start, stop));
    }

    public CompletableFuture<List<ScoredMember>> zrangeWithScores(String key, long start, long stop, ZRangeOptions options) {
        return queue(Calls.zrangeWithScores(key, start, stop, options));
    }

    public CompletableFuture<List<String>> zrevrange(String key, long start, long stop) {
        return queue(Calls.zrevrange(key, start, stop));
    }

    public CompletableFuture<List<ScoredMember>> zrevrangeWithScores(String key, long start, long stop) {
        return queue(Calls.zrevrangeWithScores(key, start, stop));
    }

    public CompletableFuture<List<String>> zrangeByScore(String key, String min, String max) {
        return queue(Calls.zrangeByScore(key, min, max));
    }

    public CompletableFuture<List<String>> zrangeByScore(String key, String min, String max, ScoreRangeOptions options) {
        return queue(Calls.zrangeByScore(key, min, max, options));
    }

    public CompletableFuture<List<ScoredMember>> zrangeByScoreWithScores(String key, String min, String max) {
        return queue(Calls.zrangeByScoreWithScores(key, min, max));
    }

    public CompletableFuture<List<ScoredMember>> zrangeByScoreWithScores(String key, String min, String max, ScoreRangeOptions options) {
        return queue(Calls.zrangeByScoreWithScores(key, min, max, options));
    }

    public CompletableFuture<List<String>> zrevrangeByScore(String key, String max, String min) {
        return queue(Calls.zrevrangeByScore(key, max, min));
    }

    public CompletableFuture<List<String>> zrevrangeByScore(String key, String max, String min, ScoreRangeOptions options) {
        return queue(Calls.zrevrangeByScore(key, max, min, options));
    }

    public CompletableFuture<List<ScoredMember>> zrevrangeByScoreWithScores(String key, String max, String min) {
        return queue(Calls.zrevrangeByScoreWithScores(key, max, min));
    }

    public CompletableFuture<List<ScoredMember>> zrevrangeByScoreWithScores(String key, String max, String min, ScoreRangeOptions options) {
        return queue(Calls.zrevrangeByScoreWithScores(key, max, min, options));
    }

    public CompletableFuture<Long> zremrangeByScore(String key, String min, String max) {
        return queue(Calls.zremrangeByScore(key, min, max));
    }

    public CompletableFuture<Long> zremrangeByRank(String key, long start, long stop) {
        return queue(Calls.zremrangeByRank(key, start, stop));
    }

    public CompletableFuture<Long> zrem(String key, String member, String... more) {
        return queue(Calls.zrem(key, member, more));
    }

    public CompletableFuture<ZScan> zscan(String key, String cursor) {
        return queue(Calls.zscan(key, cursor));
    }

    public CompletableFuture<ZScan> zscan(String key, String cursor, ScanOptions options) {
        return queue(Calls.zscan(key, cursor, options));
    }

    public CompletableFuture<String> type(String key) {
        return queue(Calls.type(key));
    }

    public CompletableFuture<Long> memoryUsage(String key) {
        return queue(Calls.memoryUsage(key));
    }

    public CompletableFuture<Long> memoryUsage(String key, MemoryUsageOptions options) {
        return queue(Calls.memoryUsage(key, options));
    }

    public CompletableFuture<Map<String, Long>> memoryStats() {
        return queue(Calls.memoryStats());
    }

    public CompletableFuture<String> objectEncoding(String key) {
        return queue(Calls.objectEncoding(key));
    }

    public CompletableFuture<List<String>> keys(String pattern) {
        return queue(Calls.keys(pattern));
    }

    public CompletableFuture<KeyScan> scan(String cursor) {
        return queue(Calls.scan(cursor));
    }

    public CompletableFuture<KeyScan> scan(String cursor, ScanOptions options) {
        return queue(Calls.scan(cursor, options));
    }

    public CompletableFuture<Long> del(String... keys) {
        return queue(Calls.del(keys));
    }

    public CompletableFuture<Long> exists(String... keys) {
        return queue(Calls.exists(keys));
    }

    public CompletableFuture<Long> expire(String key, long seconds) {
        return queue(Calls.expire(key, seconds));
    }

    public CompletableFuture<Long> expire(String key, long seconds, ExpireOptions options) {
        return queue(Calls.expire(key, seconds, options));
    }

    public CompletableFuture<Long> pexpire(String key, long milliseconds) {
        return queue(Calls.pexpire(key, milliseconds));
    }

    public CompletableFuture<Long> pexpire(String key, long milliseconds, ExpireOptions options) {
        return queue(Calls.pexpire(key, milliseconds, options));
    }

    public CompletableFuture<Long> expireat(String key, long unixSeconds) {
        return queue(Calls.expireat(key, unixSeconds));
    }

    public CompletableFuture<Long> expireat(String key, long unixSeconds, ExpireOptions options) {
        return queue(Calls.expireat(key, unixSeconds, options));
    }

    public CompletableFuture<Long> pexpireat(String key, long unixMilliseconds) {
        return queue(Calls.pexpireat(key, unixMilliseconds));
    }

    public CompletableFuture<Long> pexpireat(String key, long unixMilliseconds, ExpireOptions options) {
        return queue(Calls.pexpireat(key, unixMilliseconds, options));
    }

    public CompletableFuture<Long> persist(String key) {
        return queue(Calls.persist(key));
    }

    public CompletableFuture<Long> ttl(String key) {
        return queue(Calls.ttl(key));
    }

    public CompletableFuture<Long> pttl(String key) {
        return queue(Calls.pttl(key));
    }

    public CompletableFuture<Long> pfadd(String key, String... elements) {
        return queue(Calls.pfadd(key, elements));
    }

    public CompletableFuture<Long> pfcount(String... keys) {
        return queue(Calls.pfcount(keys));
    }

    public CompletableFuture<String> pfmerge(String destination, String... sources) {
        return queue(Calls.pfmerge(destination, sources));
    }
}
