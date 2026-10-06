package yier.bubu.redis.client;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 同一条连接上的管道。命令立刻写出，回复留到句柄或 {@link #sync()} 再读。
 * <p>
 * 取任何一个还没读的句柄，会按发送顺序读完当时已经写出的全部回复，每条单独计超时。
 * 服务端错误只从对应句柄的 {@link Reply#get()} 抛出。
 * 原始 {@code MULTI} 在写出前拒绝，管道不能同时进入事务模式。
 * 套接字上还有没读走的回复时 {@link #close()} 关掉连接；已经读进句柄的结果，包括服务端错误，不算未读。
 */
public final class Pipeline implements AutoCloseable {
    private final Connection connection;
    private final Deque<Reply<?>> replies = new ArrayDeque<>();
    private boolean finished;
    private RuntimeException broken;

    Pipeline(Connection connection) {
        this.connection = connection;
    }

    public Reply<Object> command(String... args) {
        return enqueue(Call.raw(args));
    }

    /**
     * 读完还没取的回复，已保存的服务端错误不从这里抛出。之后仍可在这个对象上再写一批。
     */
    public void sync() {
        ensureOpen();
        readOutstanding(defaultTimeoutMillis());
    }

    /**
     * 没有未读回复时连接回到普通模式并保持打开。还有未读回复时关掉连接。
     */
    @Override
    public void close() {
        if (finished) {
            return;
        }
        finished = true;
        connection.endPipeline(this, !replies.isEmpty());
    }

    long defaultTimeoutMillis() {
        return connection.commandTimeoutMillis();
    }

    <T> Reply<T> enqueue(Call<T> call) {
        ensureOpen();
        // 先编完再占一个回复位。UTF-8 失败时套接字上还没有这条命令，句柄也不存在。
        connection.writeCommand(defaultTimeoutMillis(), call.args);
        Reply<T> reply = new Reply<>(this, call);
        replies.add(reply);
        return reply;
    }

    void readThrough(Reply<?> target, long timeoutMillis) {
        if (target.isRead()) {
            return;
        }
        if (broken != null) {
            throw broken;
        }
        if (connection.isClosed()) {
            throw new IllegalStateException("connection is closed");
        }
        // 读完目标之后仍读走其余已写出的回复；已完成句柄自己持有结果，不再由管道保留。
        readOutstanding(timeoutMillis);
        if (!target.isRead()) {
            throw new IllegalStateException("pipeline reply was not read");
        }
    }

    private void readOutstanding(long timeoutMillis) {
        while (!replies.isEmpty()) {
            readOne(replies.getFirst(), timeoutMillis);
            replies.removeFirst();
        }
    }

    private void readOne(Reply<?> reply, long timeoutMillis) {
        if (broken != null) {
            throw broken;
        }
        try {
            Object raw = connection.readCommandReply(timeoutMillis);
            connection.noteSuccessfulCommand(reply.args());
            try {
                reply.complete(raw);
            } catch (DecodeException failure) {
                reply.fail(failure);
            }
        } catch (ServerException failure) {
            reply.fail(failure);
        } catch (DecodeException failure) {
            reply.fail(failure);
        } catch (CommandTimeoutException | ConnectionException failure) {
            // 读失败已经关掉连接。还没保存结果的句柄随后取的时候抛这次失败，不再重试。
            broken = failure;
            throw failure;
        }
    }

    private void ensureOpen() {
        if (broken != null) {
            throw broken;
        }
        if (connection.isClosed()) {
            throw new IllegalStateException("connection is closed");
        }
        if (finished || connection.currentPipeline() != this) {
            throw new IllegalStateException("pipeline is closed");
        }
    }

    public Reply<String> ping() {
        return enqueue(Calls.ping());
    }

    public Reply<String> ping(String message) {
        return enqueue(Calls.ping(message));
    }

    public Reply<String> echo(String message) {
        return enqueue(Calls.echo(message));
    }

    public Reply<List<CommandInfo>> commandList() {
        return enqueue(Calls.commandList());
    }

    public Reply<Long> commandCount() {
        return enqueue(Calls.commandCount());
    }

    public Reply<List<CommandInfo>> commandInfo(String... names) {
        return enqueue(Calls.commandInfo(names));
    }

    public Reply<String> select(int index) {
        return enqueue(Calls.select(index));
    }

    public Reply<String> quit() {
        return enqueue(Calls.quit());
    }

    public Reply<String> clientSetname(String name) {
        return enqueue(Calls.clientSetname(name));
    }

    public Reply<String> clientGetname() {
        return enqueue(Calls.clientGetname());
    }

    public Reply<String> clientSetinfo(String attribute, String value) {
        return enqueue(Calls.clientSetinfo(attribute, value));
    }

    public Reply<Object> auth(String password) {
        return enqueue(Calls.auth(password));
    }

    public Reply<Object> auth(String username, String password) {
        return enqueue(Calls.auth(username, password));
    }

    public Reply<String> flushdb() {
        return enqueue(Calls.flushdb());
    }

    public Reply<String> flushdb(FlushMode mode) {
        return enqueue(Calls.flushdb(mode));
    }

    public Reply<String> info() {
        return enqueue(Calls.info());
    }

    public Reply<String> info(String... sections) {
        return enqueue(Calls.info(sections));
    }

    public Reply<Map<String, Object>> infoHealth() {
        return enqueue(Calls.infoHealth());
    }

    public Reply<Map<String, Object>> infoYierdis() {
        return enqueue(Calls.infoYierdis());
    }

    public Reply<Map<String, Object>> stats() {
        return enqueue(Calls.stats());
    }

    public Reply<String> ydreconcile() {
        return enqueue(Calls.ydreconcile());
    }

    public Reply<String> set(String key, String value) {
        return enqueue(Calls.set(key, value));
    }

    public Reply<String> set(String key, String value, SetOptions options) {
        return enqueue(Calls.set(key, value, options));
    }

    public Reply<String> setGet(String key, String value) {
        return enqueue(Calls.setGet(key, value));
    }

    public Reply<String> setGet(String key, String value, SetGetOptions options) {
        return enqueue(Calls.setGet(key, value, options));
    }

    public Reply<String> get(String key) {
        return enqueue(Calls.get(key));
    }

    public Reply<Long> strlen(String key) {
        return enqueue(Calls.strlen(key));
    }

    public Reply<Long> append(String key, String value) {
        return enqueue(Calls.append(key, value));
    }

    public Reply<Long> setbit(String key, long offset, long value) {
        return enqueue(Calls.setbit(key, offset, value));
    }

    public Reply<Long> getbit(String key, long offset) {
        return enqueue(Calls.getbit(key, offset));
    }

    public Reply<Long> bitcount(String key) {
        return enqueue(Calls.bitcount(key));
    }

    public Reply<Long> bitcount(String key, long start, long end) {
        return enqueue(Calls.bitcount(key, start, end));
    }

    public Reply<Long> incr(String key) {
        return enqueue(Calls.incr(key));
    }

    public Reply<Long> decr(String key) {
        return enqueue(Calls.decr(key));
    }

    public Reply<Long> hset(String key, String field, String value, String... more) {
        return enqueue(Calls.hset(key, field, value, more));
    }

    public Reply<String> hget(String key, String field) {
        return enqueue(Calls.hget(key, field));
    }

    public Reply<Map<String, String>> hgetall(String key) {
        return enqueue(Calls.hgetall(key));
    }

    public Reply<Long> hlen(String key) {
        return enqueue(Calls.hlen(key));
    }

    public Reply<Long> hdel(String key, String field, String... more) {
        return enqueue(Calls.hdel(key, field, more));
    }

    public Reply<HashScan> hscan(String key, String cursor) {
        return enqueue(Calls.hscan(key, cursor));
    }

    public Reply<HashScan> hscan(String key, String cursor, ScanOptions options) {
        return enqueue(Calls.hscan(key, cursor, options));
    }

    public Reply<HashFieldScan> hscanNoValues(String key, String cursor) {
        return enqueue(Calls.hscanNoValues(key, cursor));
    }

    public Reply<HashFieldScan> hscanNoValues(String key, String cursor, ScanOptions options) {
        return enqueue(Calls.hscanNoValues(key, cursor, options));
    }

    public Reply<Long> lpush(String key, String value, String... more) {
        return enqueue(Calls.lpush(key, value, more));
    }

    public Reply<Long> rpush(String key, String value, String... more) {
        return enqueue(Calls.rpush(key, value, more));
    }

    public Reply<List<String>> lrange(String key, long start, long stop) {
        return enqueue(Calls.lrange(key, start, stop));
    }

    public Reply<String> lpop(String key) {
        return enqueue(Calls.lpop(key));
    }

    public Reply<List<String>> lpop(String key, long count) {
        return enqueue(Calls.lpop(key, count));
    }

    public Reply<String> rpop(String key) {
        return enqueue(Calls.rpop(key));
    }

    public Reply<List<String>> rpop(String key, long count) {
        return enqueue(Calls.rpop(key, count));
    }

    public Reply<Long> sadd(String key, String member, String... more) {
        return enqueue(Calls.sadd(key, member, more));
    }

    public Reply<Long> srem(String key, String member, String... more) {
        return enqueue(Calls.srem(key, member, more));
    }

    public Reply<Set<String>> smembers(String key) {
        return enqueue(Calls.smembers(key));
    }

    public Reply<Long> sismember(String key, String member) {
        return enqueue(Calls.sismember(key, member));
    }

    public Reply<Long> scard(String key) {
        return enqueue(Calls.scard(key));
    }

    public Reply<SetScan> sscan(String key, String cursor) {
        return enqueue(Calls.sscan(key, cursor));
    }

    public Reply<SetScan> sscan(String key, String cursor, ScanOptions options) {
        return enqueue(Calls.sscan(key, cursor, options));
    }

    public Reply<Long> zadd(String key, String score, String member, String... more) {
        return enqueue(Calls.zadd(key, score, member, more));
    }

    public Reply<Long> zadd(String key, ZAddOptions options, String score, String member, String... more) {
        return enqueue(Calls.zadd(key, options, score, member, more));
    }

    public Reply<String> zaddIncr(String key, String score, String member) {
        return enqueue(Calls.zaddIncr(key, score, member));
    }

    public Reply<String> zaddIncr(String key, ZAddIncrOptions options, String score, String member) {
        return enqueue(Calls.zaddIncr(key, options, score, member));
    }

    public Reply<List<String>> zrange(String key, long start, long stop) {
        return enqueue(Calls.zrange(key, start, stop));
    }

    public Reply<List<String>> zrange(String key, long start, long stop, ZRangeOptions options) {
        return enqueue(Calls.zrange(key, start, stop, options));
    }

    public Reply<List<ScoredMember>> zrangeWithScores(String key, long start, long stop) {
        return enqueue(Calls.zrangeWithScores(key, start, stop));
    }

    public Reply<List<ScoredMember>> zrangeWithScores(
            String key,
            long start,
            long stop,
            ZRangeOptions options
    ) {
        return enqueue(Calls.zrangeWithScores(key, start, stop, options));
    }

    public Reply<List<String>> zrevrange(String key, long start, long stop) {
        return enqueue(Calls.zrevrange(key, start, stop));
    }

    public Reply<List<ScoredMember>> zrevrangeWithScores(String key, long start, long stop) {
        return enqueue(Calls.zrevrangeWithScores(key, start, stop));
    }

    public Reply<List<String>> zrangeByScore(String key, String min, String max) {
        return enqueue(Calls.zrangeByScore(key, min, max));
    }

    public Reply<List<String>> zrangeByScore(String key, String min, String max, ScoreRangeOptions options) {
        return enqueue(Calls.zrangeByScore(key, min, max, options));
    }

    public Reply<List<ScoredMember>> zrangeByScoreWithScores(String key, String min, String max) {
        return enqueue(Calls.zrangeByScoreWithScores(key, min, max));
    }

    public Reply<List<ScoredMember>> zrangeByScoreWithScores(
            String key,
            String min,
            String max,
            ScoreRangeOptions options
    ) {
        return enqueue(Calls.zrangeByScoreWithScores(key, min, max, options));
    }

    public Reply<List<String>> zrevrangeByScore(String key, String max, String min) {
        return enqueue(Calls.zrevrangeByScore(key, max, min));
    }

    public Reply<List<String>> zrevrangeByScore(
            String key,
            String max,
            String min,
            ScoreRangeOptions options
    ) {
        return enqueue(Calls.zrevrangeByScore(key, max, min, options));
    }

    public Reply<List<ScoredMember>> zrevrangeByScoreWithScores(String key, String max, String min) {
        return enqueue(Calls.zrevrangeByScoreWithScores(key, max, min));
    }

    public Reply<List<ScoredMember>> zrevrangeByScoreWithScores(
            String key,
            String max,
            String min,
            ScoreRangeOptions options
    ) {
        return enqueue(Calls.zrevrangeByScoreWithScores(key, max, min, options));
    }

    public Reply<Long> zremrangeByScore(String key, String min, String max) {
        return enqueue(Calls.zremrangeByScore(key, min, max));
    }

    public Reply<Long> zremrangeByRank(String key, long start, long stop) {
        return enqueue(Calls.zremrangeByRank(key, start, stop));
    }

    public Reply<Long> zrem(String key, String member, String... more) {
        return enqueue(Calls.zrem(key, member, more));
    }

    public Reply<ZScan> zscan(String key, String cursor) {
        return enqueue(Calls.zscan(key, cursor));
    }

    public Reply<ZScan> zscan(String key, String cursor, ScanOptions options) {
        return enqueue(Calls.zscan(key, cursor, options));
    }

    public Reply<String> type(String key) {
        return enqueue(Calls.type(key));
    }

    public Reply<Long> memoryUsage(String key) {
        return enqueue(Calls.memoryUsage(key));
    }

    public Reply<Long> memoryUsage(String key, MemoryUsageOptions options) {
        return enqueue(Calls.memoryUsage(key, options));
    }

    public Reply<Map<String, Long>> memoryStats() {
        return enqueue(Calls.memoryStats());
    }

    public Reply<String> objectEncoding(String key) {
        return enqueue(Calls.objectEncoding(key));
    }

    public Reply<List<String>> keys(String pattern) {
        return enqueue(Calls.keys(pattern));
    }

    public Reply<KeyScan> scan(String cursor) {
        return enqueue(Calls.scan(cursor));
    }

    public Reply<KeyScan> scan(String cursor, ScanOptions options) {
        return enqueue(Calls.scan(cursor, options));
    }

    public Reply<Long> del(String... keys) {
        return enqueue(Calls.del(keys));
    }

    public Reply<Long> exists(String... keys) {
        return enqueue(Calls.exists(keys));
    }

    public Reply<Long> expire(String key, long seconds) {
        return enqueue(Calls.expire(key, seconds));
    }

    public Reply<Long> expire(String key, long seconds, ExpireOptions options) {
        return enqueue(Calls.expire(key, seconds, options));
    }

    public Reply<Long> pexpire(String key, long milliseconds) {
        return enqueue(Calls.pexpire(key, milliseconds));
    }

    public Reply<Long> pexpire(String key, long milliseconds, ExpireOptions options) {
        return enqueue(Calls.pexpire(key, milliseconds, options));
    }

    public Reply<Long> expireat(String key, long unixSeconds) {
        return enqueue(Calls.expireat(key, unixSeconds));
    }

    public Reply<Long> expireat(String key, long unixSeconds, ExpireOptions options) {
        return enqueue(Calls.expireat(key, unixSeconds, options));
    }

    public Reply<Long> pexpireat(String key, long unixMilliseconds) {
        return enqueue(Calls.pexpireat(key, unixMilliseconds));
    }

    public Reply<Long> pexpireat(String key, long unixMilliseconds, ExpireOptions options) {
        return enqueue(Calls.pexpireat(key, unixMilliseconds, options));
    }

    public Reply<Long> persist(String key) {
        return enqueue(Calls.persist(key));
    }

    public Reply<Long> ttl(String key) {
        return enqueue(Calls.ttl(key));
    }

    public Reply<Long> pttl(String key) {
        return enqueue(Calls.pttl(key));
    }

    public Reply<Long> pfadd(String key, String... elements) {
        return enqueue(Calls.pfadd(key, elements));
    }

    public Reply<Long> pfcount(String... keys) {
        return enqueue(Calls.pfcount(keys));
    }

    public Reply<String> pfmerge(String destination, String... sources) {
        return enqueue(Calls.pfmerge(destination, sources));
    }
}
