package yier.bubu.redis.client;

import java.util.ArrayList;
import java.util.List;

/**
 * 同一条连接上的事务。{@code MULTI} 已在 {@link Connection#multi()} 里送出。
 * <p>
 * 类型化命令立刻发送，回复是 {@code QUEUED} 时返回。原始 {@link #command(String...)} 返回当时的 RESP 值，
 * 入队成功时是字符串 {@code QUEUED}。{@link #exec()} 按入队顺序给出结果；其中的单条错误是
 * {@link ServerException}，不向外抛。{@code EXEC} 自身失败会抛出并结束这个对象。
 * {@link #discard()} 成功后同样结束。
 */
public final class Transaction {
    private final Connection connection;
    private final List<Call<?>> queued = new ArrayList<>();
    private boolean finished;

    Transaction(Connection connection) {
        this.connection = connection;
    }

    /**
     * 立刻发送。入队成功时返回服务端回复，包括字符串 {@code QUEUED}。
     * 入队期的服务端错误抛出，连接保持事务模式，这条不进入后来的 {@link #exec()} 列表。
     */
    public Object command(String... args) {
        return queue(Call.raw(args));
    }

    /**
     * 发送 {@code EXEC}。空队列也发送，回复空数组时返回空列表。
     * 数组元素里的错误是 {@link ServerException}。{@code EXEC} 自身的错误，包括 {@code EXECABORT}，抛出并结束这个对象。
     */
    public List<Object> exec() {
        ensureActive();
        List<Call<?>> batch = new ArrayList<>(queued);
        queued.clear();
        try {
            List<Object> results = connection.collectExec(batch);
            connection.finishTransaction(this);
            connection.noteQueuedResults(batch, results);
            return results;
        } catch (RuntimeException failure) {
            connection.finishTransaction(this);
            throw failure;
        }
    }

    /**
     * 发送 {@code DISCARD}。{@code OK} 结束这个对象并让连接回到普通模式。
     * 服务端错误抛出，不把事务当成已经结束。
     */
    public void discard() {
        ensureActive();
        connection.writeCommand(connection.commandTimeoutMillis(), new String[]{"DISCARD"});
        Object reply = connection.readCommandReply(connection.commandTimeoutMillis());
        if (!"OK".equals(reply)) {
            String actual = reply == null ? "null" : reply.getClass().getSimpleName();
            throw new DecodeException("reply was " + actual + ", expected OK", null);
        }
        connection.finishTransaction(this);
    }

    void markFinished() {
        finished = true;
    }

    private Object queue(Call<?> call) {
        ensureActive();
        // 参数编不成 UTF-8 时在写出前抛出。事务模式保持不变，这条也不进入 EXEC 列表。
        connection.writeCommand(connection.commandTimeoutMillis(), call.args);
        Object reply;
        try {
            reply = connection.readCommandReply(connection.commandTimeoutMillis());
        } catch (ServerException failure) {
            // 入队期的 - 回复不是 EXEC 数组里的一项。连接留在事务模式，直到 exec 或 discard。
            throw failure;
        }
        if (!(reply instanceof String text) || !"QUEUED".equals(text)) {
            String actual = reply == null ? "null" : reply.getClass().getSimpleName();
            throw new DecodeException("reply was " + actual + ", expected QUEUED", null);
        }
        queued.add(call);
        return reply;
    }

    private void ensureActive() {
        if (connection.isClosed()) {
            throw new IllegalStateException("connection is closed");
        }
        if (finished || connection.currentTransaction() != this) {
            throw new IllegalStateException("transaction is finished");
        }
    }

    public void ping() {
        queue(Calls.ping());
    }

    public void ping(String message) {
        queue(Calls.ping(message));
    }

    public void echo(String message) {
        queue(Calls.echo(message));
    }

    public void commandList() {
        queue(Calls.commandList());
    }

    public void commandCount() {
        queue(Calls.commandCount());
    }

    public void commandInfo(String... names) {
        queue(Calls.commandInfo(names));
    }

    public void select(int index) {
        queue(Calls.select(index));
    }

    public void quit() {
        queue(Calls.quit());
    }

    public void clientSetname(String name) {
        queue(Calls.clientSetname(name));
    }

    public void clientGetname() {
        queue(Calls.clientGetname());
    }

    public void clientSetinfo(String attribute, String value) {
        queue(Calls.clientSetinfo(attribute, value));
    }

    public void auth(String password) {
        queue(Calls.auth(password));
    }

    public void auth(String username, String password) {
        queue(Calls.auth(username, password));
    }

    public void flushdb() {
        queue(Calls.flushdb());
    }

    public void flushdb(FlushMode mode) {
        queue(Calls.flushdb(mode));
    }

    public void info() {
        queue(Calls.info());
    }

    public void info(String... sections) {
        queue(Calls.info(sections));
    }

    public void infoHealth() {
        queue(Calls.infoHealth());
    }

    public void infoYierdis() {
        queue(Calls.infoYierdis());
    }

    public void stats() {
        queue(Calls.stats());
    }

    public void ydreconcile() {
        queue(Calls.ydreconcile());
    }

    public void set(String key, String value) {
        queue(Calls.set(key, value));
    }

    public void set(String key, String value, SetOptions options) {
        queue(Calls.set(key, value, options));
    }

    public void setGet(String key, String value) {
        queue(Calls.setGet(key, value));
    }

    public void setGet(String key, String value, SetGetOptions options) {
        queue(Calls.setGet(key, value, options));
    }

    public void get(String key) {
        queue(Calls.get(key));
    }

    public void strlen(String key) {
        queue(Calls.strlen(key));
    }

    public void append(String key, String value) {
        queue(Calls.append(key, value));
    }

    public void setbit(String key, long offset, long value) {
        queue(Calls.setbit(key, offset, value));
    }

    public void getbit(String key, long offset) {
        queue(Calls.getbit(key, offset));
    }

    public void bitcount(String key) {
        queue(Calls.bitcount(key));
    }

    public void bitcount(String key, long start, long end) {
        queue(Calls.bitcount(key, start, end));
    }

    public void incr(String key) {
        queue(Calls.incr(key));
    }

    public void decr(String key) {
        queue(Calls.decr(key));
    }

    public void hset(String key, String field, String value, String... more) {
        queue(Calls.hset(key, field, value, more));
    }

    public void hget(String key, String field) {
        queue(Calls.hget(key, field));
    }

    public void hgetall(String key) {
        queue(Calls.hgetall(key));
    }

    public void hlen(String key) {
        queue(Calls.hlen(key));
    }

    public void hdel(String key, String field, String... more) {
        queue(Calls.hdel(key, field, more));
    }

    public void hscan(String key, String cursor) {
        queue(Calls.hscan(key, cursor));
    }

    public void hscan(String key, String cursor, ScanOptions options) {
        queue(Calls.hscan(key, cursor, options));
    }

    public void hscanNoValues(String key, String cursor) {
        queue(Calls.hscanNoValues(key, cursor));
    }

    public void hscanNoValues(String key, String cursor, ScanOptions options) {
        queue(Calls.hscanNoValues(key, cursor, options));
    }

    public void lpush(String key, String value, String... more) {
        queue(Calls.lpush(key, value, more));
    }

    public void rpush(String key, String value, String... more) {
        queue(Calls.rpush(key, value, more));
    }

    public void lrange(String key, long start, long stop) {
        queue(Calls.lrange(key, start, stop));
    }

    public void lpop(String key) {
        queue(Calls.lpop(key));
    }

    public void lpop(String key, long count) {
        queue(Calls.lpop(key, count));
    }

    public void rpop(String key) {
        queue(Calls.rpop(key));
    }

    public void rpop(String key, long count) {
        queue(Calls.rpop(key, count));
    }

    public void sadd(String key, String member, String... more) {
        queue(Calls.sadd(key, member, more));
    }

    public void srem(String key, String member, String... more) {
        queue(Calls.srem(key, member, more));
    }

    public void smembers(String key) {
        queue(Calls.smembers(key));
    }

    public void sismember(String key, String member) {
        queue(Calls.sismember(key, member));
    }

    public void scard(String key) {
        queue(Calls.scard(key));
    }

    public void sscan(String key, String cursor) {
        queue(Calls.sscan(key, cursor));
    }

    public void sscan(String key, String cursor, ScanOptions options) {
        queue(Calls.sscan(key, cursor, options));
    }

    public void zadd(String key, String score, String member, String... more) {
        queue(Calls.zadd(key, score, member, more));
    }

    public void zadd(String key, ZAddOptions options, String score, String member, String... more) {
        queue(Calls.zadd(key, options, score, member, more));
    }

    public void zaddIncr(String key, String score, String member) {
        queue(Calls.zaddIncr(key, score, member));
    }

    public void zaddIncr(String key, ZAddIncrOptions options, String score, String member) {
        queue(Calls.zaddIncr(key, options, score, member));
    }

    public void zrange(String key, long start, long stop) {
        queue(Calls.zrange(key, start, stop));
    }

    public void zrange(String key, long start, long stop, ZRangeOptions options) {
        queue(Calls.zrange(key, start, stop, options));
    }

    public void zrangeWithScores(String key, long start, long stop) {
        queue(Calls.zrangeWithScores(key, start, stop));
    }

    public void zrangeWithScores(String key, long start, long stop, ZRangeOptions options) {
        queue(Calls.zrangeWithScores(key, start, stop, options));
    }

    public void zrevrange(String key, long start, long stop) {
        queue(Calls.zrevrange(key, start, stop));
    }

    public void zrevrangeWithScores(String key, long start, long stop) {
        queue(Calls.zrevrangeWithScores(key, start, stop));
    }

    public void zrangeByScore(String key, String min, String max) {
        queue(Calls.zrangeByScore(key, min, max));
    }

    public void zrangeByScore(String key, String min, String max, ScoreRangeOptions options) {
        queue(Calls.zrangeByScore(key, min, max, options));
    }

    public void zrangeByScoreWithScores(String key, String min, String max) {
        queue(Calls.zrangeByScoreWithScores(key, min, max));
    }

    public void zrangeByScoreWithScores(String key, String min, String max, ScoreRangeOptions options) {
        queue(Calls.zrangeByScoreWithScores(key, min, max, options));
    }

    public void zrevrangeByScore(String key, String max, String min) {
        queue(Calls.zrevrangeByScore(key, max, min));
    }

    public void zrevrangeByScore(String key, String max, String min, ScoreRangeOptions options) {
        queue(Calls.zrevrangeByScore(key, max, min, options));
    }

    public void zrevrangeByScoreWithScores(String key, String max, String min) {
        queue(Calls.zrevrangeByScoreWithScores(key, max, min));
    }

    public void zrevrangeByScoreWithScores(String key, String max, String min, ScoreRangeOptions options) {
        queue(Calls.zrevrangeByScoreWithScores(key, max, min, options));
    }

    public void zremrangeByScore(String key, String min, String max) {
        queue(Calls.zremrangeByScore(key, min, max));
    }

    public void zremrangeByRank(String key, long start, long stop) {
        queue(Calls.zremrangeByRank(key, start, stop));
    }

    public void zrem(String key, String member, String... more) {
        queue(Calls.zrem(key, member, more));
    }

    public void zscan(String key, String cursor) {
        queue(Calls.zscan(key, cursor));
    }

    public void zscan(String key, String cursor, ScanOptions options) {
        queue(Calls.zscan(key, cursor, options));
    }

    public void type(String key) {
        queue(Calls.type(key));
    }

    public void memoryUsage(String key) {
        queue(Calls.memoryUsage(key));
    }

    public void memoryUsage(String key, MemoryUsageOptions options) {
        queue(Calls.memoryUsage(key, options));
    }

    public void memoryStats() {
        queue(Calls.memoryStats());
    }

    public void objectEncoding(String key) {
        queue(Calls.objectEncoding(key));
    }

    public void keys(String pattern) {
        queue(Calls.keys(pattern));
    }

    public void scan(String cursor) {
        queue(Calls.scan(cursor));
    }

    public void scan(String cursor, ScanOptions options) {
        queue(Calls.scan(cursor, options));
    }

    public void del(String... keys) {
        queue(Calls.del(keys));
    }

    public void exists(String... keys) {
        queue(Calls.exists(keys));
    }

    public void expire(String key, long seconds) {
        queue(Calls.expire(key, seconds));
    }

    public void expire(String key, long seconds, ExpireOptions options) {
        queue(Calls.expire(key, seconds, options));
    }

    public void pexpire(String key, long milliseconds) {
        queue(Calls.pexpire(key, milliseconds));
    }

    public void pexpire(String key, long milliseconds, ExpireOptions options) {
        queue(Calls.pexpire(key, milliseconds, options));
    }

    public void expireat(String key, long unixSeconds) {
        queue(Calls.expireat(key, unixSeconds));
    }

    public void expireat(String key, long unixSeconds, ExpireOptions options) {
        queue(Calls.expireat(key, unixSeconds, options));
    }

    public void pexpireat(String key, long unixMilliseconds) {
        queue(Calls.pexpireat(key, unixMilliseconds));
    }

    public void pexpireat(String key, long unixMilliseconds, ExpireOptions options) {
        queue(Calls.pexpireat(key, unixMilliseconds, options));
    }

    public void persist(String key) {
        queue(Calls.persist(key));
    }

    public void ttl(String key) {
        queue(Calls.ttl(key));
    }

    public void pttl(String key) {
        queue(Calls.pttl(key));
    }

    public void pfadd(String key, String... elements) {
        queue(Calls.pfadd(key, elements));
    }

    public void pfcount(String... keys) {
        queue(Calls.pfcount(keys));
    }

    public void pfmerge(String destination, String... sources) {
        queue(Calls.pfmerge(destination, sources));
    }
}
