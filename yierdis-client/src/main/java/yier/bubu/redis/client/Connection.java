package yier.bubu.redis.client;

import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 指向一台 standalone Yierdis 的阻塞连接。同一时刻只由一个调用方使用。
 * <p>
 * 原始命令返回 {@code String}、{@code Long}、{@code null} 或嵌套 {@code List}。
 * 类型化方法在此之上检查回复形状；形状不对抛出 {@link DecodeException}，回复已经读完，连接可以继续用。
 * 带 {@code commandTimeoutMillis} 的重载只作用于这一次调用。
 * {@code HELLO} 没有类型化方法。{@link #pipeline()} 和 {@link #multi()} 把这条连接切到管道或事务；
 * {@code EXEC} 和 {@code DISCARD} 在事务对象上。
 * <p>
 * 服务端错误和 UTF-8 解码失败都读完当前回复，连接可以继续用。
 * 读超时、读写失败、超过 512 MiB 的 bulk，以及顶层 {@code %}、{@code ~}、{@code _} 会关掉连接，并且不自动重试。
 */
public final class Connection implements AutoCloseable {
    private enum Mode {
        NORMAL,
        PIPELINE,
        TRANSACTION
    }

    private final Socket socket;
    private final PushbackInputStream in;
    private final OutputStream out;
    private final long commandTimeoutMillis;
    // 同一条连接只处在普通、管道、事务之一。管道和事务是这条通道上的对象，不是第二条连接。
    private Mode mode = Mode.NORMAL;
    private Pipeline pipeline;
    private Transaction transaction;
    private int database;
    private boolean closed;

    private Connection(Socket socket, long commandTimeoutMillis) throws IOException {
        this.socket = socket;
        this.commandTimeoutMillis = commandTimeoutMillis;
        this.in = new PushbackInputStream(socket.getInputStream(), 1);
        this.out = socket.getOutputStream();
    }

    public static Connection connect() {
        return connect(ConnectionSettings.defaults());
    }

    public static Connection connect(String host, int port) {
        return connect(ConnectionSettings.defaults().withHost(host).withPort(port));
    }

    public static Connection connect(ConnectionSettings settings) {
        Objects.requireNonNull(settings, "settings");
        Socket socket = new Socket();
        try {
            socket.setTcpNoDelay(true);
            socket.connect(
                    new InetSocketAddress(settings.host(), settings.port()),
                    toSocketTimeoutMillis(settings.connectTimeoutMillis())
            );
            Connection connection = new Connection(socket, settings.commandTimeoutMillis());
            try {
                if (settings.database() != 0) {
                    // 非 0 的 DB 要在把连接交给调用方之前 SELECT 成功。失败时关掉 socket，调用方拿不到这条连接。
                    connection.command("SELECT", Integer.toString(settings.database()));
                }
                return connection;
            } catch (Throwable failure) {
                connection.close();
                throw failure;
            }
        } catch (IOException e) {
            closeQuietly(socket);
            throw new ConnectionException(
                    "failed to connect to " + settings.host() + ":" + settings.port(), e);
        }
    }

    public Object command(String... args) {
        return command(commandTimeoutMillis, args);
    }

    public Object command(long commandTimeoutMillis, String... args) {
        if (closed) {
            throw new IllegalStateException("connection is closed");
        }
        // 管道或事务还开着时，普通命令在写出前拒绝，避免把字节写进尚未配对的回复。
        if (mode != Mode.NORMAL) {
            throw new IllegalStateException("connection is not in normal mode");
        }
        if (commandTimeoutMillis <= 0) {
            throw new IllegalArgumentException("commandTimeoutMillis must be > 0");
        }
        writeCommand(commandTimeoutMillis, args);
        Object value = readCommandReply(commandTimeoutMillis);
        noteSuccessfulCommand(args);
        return value;
    }

    /**
     * 进入管道模式。已经在管道模式时返回当前管道对象。
     * 事务模式或连接已关闭时拒绝，并且不写出。
     */
    public Pipeline pipeline() {
        if (closed) {
            throw new IllegalStateException("connection is closed");
        }
        if (mode == Mode.PIPELINE) {
            return pipeline;
        }
        if (mode != Mode.NORMAL) {
            throw new IllegalStateException("connection is not in normal mode");
        }
        mode = Mode.PIPELINE;
        pipeline = new Pipeline(this);
        return pipeline;
    }

    /**
     * 立刻发送 {@code MULTI}。回复 {@code OK} 时进入事务模式并返回事务对象。
     * 服务端错误保持普通模式。管道模式，或已经在事务里时，在写出前拒绝，避免嵌套 {@code MULTI}。
     */
    public Transaction multi() {
        if (closed) {
            throw new IllegalStateException("connection is closed");
        }
        if (mode != Mode.NORMAL) {
            throw new IllegalStateException("connection is not in normal mode");
        }
        writeCommand(commandTimeoutMillis, new String[]{"MULTI"});
        Object reply = readCommandReply(commandTimeoutMillis);
        if (!"OK".equals(reply)) {
            String actual = reply == null ? "null" : reply.getClass().getSimpleName();
            throw new DecodeException("reply was " + actual + ", expected OK", null);
        }
        mode = Mode.TRANSACTION;
        transaction = new Transaction(this);
        return transaction;
    }

    /**
     * 这条连接记录的 DB 下标。打开时的非 0 下标要等 SELECT 成功才记上；
     * 之后原始 {@code SELECT} 成功才更新，服务端错误不改变它。
     */
    public int database() {
        return database;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        // 调用方放弃连接时不补 EXEC、DISCARD 或 sync。未完成的管道和事务一起丢掉。
        mode = Mode.NORMAL;
        pipeline = null;
        transaction = null;
        closeQuietly(socket);
    }

    public String ping() {
        return ping(commandTimeoutMillis);
    }

    public String ping(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.ping());
    }

    public String ping(String message) {
        return ping(commandTimeoutMillis, message);
    }

    public String ping(long commandTimeoutMillis, String message) {
        return complete(commandTimeoutMillis, Calls.ping(message));
    }

    public String echo(String message) {
        return echo(commandTimeoutMillis, message);
    }

    public String echo(long commandTimeoutMillis, String message) {
        return complete(commandTimeoutMillis, Calls.echo(message));
    }

    public List<CommandInfo> commandList() {
        return commandList(commandTimeoutMillis);
    }

    public List<CommandInfo> commandList(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.commandList());
    }

    public long commandCount() {
        return commandCount(commandTimeoutMillis);
    }

    public long commandCount(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.commandCount());
    }

    public List<CommandInfo> commandInfo(String... names) {
        return commandInfo(commandTimeoutMillis, names);
    }

    public List<CommandInfo> commandInfo(long commandTimeoutMillis, String... names) {
        return complete(commandTimeoutMillis, Calls.commandInfo(names));
    }

    /**
     * 走原始 {@code SELECT}。成功回复返回之后，{@link #database()} 才更新。
     */
    public String select(int index) {
        return select(commandTimeoutMillis, index);
    }

    public String select(long commandTimeoutMillis, int index) {
        return complete(commandTimeoutMillis, Calls.select(index));
    }

    public String quit() {
        return quit(commandTimeoutMillis);
    }

    public String quit(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.quit());
    }

    public String clientSetname(String name) {
        return clientSetname(commandTimeoutMillis, name);
    }

    public String clientSetname(long commandTimeoutMillis, String name) {
        return complete(commandTimeoutMillis, Calls.clientSetname(name));
    }

    public String clientGetname() {
        return clientGetname(commandTimeoutMillis);
    }

    public String clientGetname(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.clientGetname());
    }

    public String clientSetinfo(String attribute, String value) {
        return clientSetinfo(commandTimeoutMillis, attribute, value);
    }

    public String clientSetinfo(long commandTimeoutMillis, String attribute, String value) {
        return complete(commandTimeoutMillis, Calls.clientSetinfo(attribute, value));
    }

    public void auth(String password) {
        auth(commandTimeoutMillis, password);
    }

    public void auth(long commandTimeoutMillis, String password) {
        complete(commandTimeoutMillis, Calls.auth(password));
    }

    public void auth(String username, String password) {
        auth(commandTimeoutMillis, username, password);
    }

    public void auth(long commandTimeoutMillis, String username, String password) {
        complete(commandTimeoutMillis, Calls.auth(username, password));
    }

    public String flushdb() {
        return flushdb(commandTimeoutMillis);
    }

    public String flushdb(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.flushdb());
    }

    public String flushdb(FlushMode mode) {
        return flushdb(commandTimeoutMillis, mode);
    }

    public String flushdb(long commandTimeoutMillis, FlushMode mode) {
        return complete(commandTimeoutMillis, Calls.flushdb(mode));
    }

    /**
     * {@code INFO} 的 bulk 文本。单独的 health / yierdis 节不是这段文本。
     */
    public String info() {
        return info(commandTimeoutMillis);
    }

    public String info(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.info());
    }

    public String info(String... sections) {
        return info(commandTimeoutMillis, sections);
    }

    public String info(long commandTimeoutMillis, String... sections) {
        return complete(commandTimeoutMillis, Calls.info(sections));
    }

    /**
     * 单独的 {@code INFO health}。这一节是字段数组，不是 {@link #info()} 的 bulk 文本。
     */
    public Map<String, Object> infoHealth() {
        return infoHealth(commandTimeoutMillis);
    }

    public Map<String, Object> infoHealth(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.infoHealth());
    }

    /**
     * 单独的 {@code INFO yierdis}。这一节是字段数组，不是 {@link #info()} 的 bulk 文本。
     */
    public Map<String, Object> infoYierdis() {
        return infoYierdis(commandTimeoutMillis);
    }

    public Map<String, Object> infoYierdis(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.infoYierdis());
    }

    /**
     * {@code STATS}。数值字段是 {@code Long}，文本字段是 {@code String}，顺序与回复一致。
     */
    public Map<String, Object> stats() {
        return stats(commandTimeoutMillis);
    }

    public Map<String, Object> stats(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.stats());
    }

    public String ydreconcile() {
        return ydreconcile(commandTimeoutMillis);
    }

    public String ydreconcile(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.ydreconcile());
    }

    public String set(String key, String value) {
        return set(commandTimeoutMillis, key, value);
    }

    public String set(long commandTimeoutMillis, String key, String value) {
        return complete(commandTimeoutMillis, Calls.set(key, value));
    }

    public String set(String key, String value, SetOptions options) {
        return set(commandTimeoutMillis, key, value, options);
    }

    public String set(long commandTimeoutMillis, String key, String value, SetOptions options) {
        return complete(commandTimeoutMillis, Calls.set(key, value, options));
    }

    /**
     * {@code SET GET}。回复是旧值，或原来没有值时的 null。
     */
    public String setGet(String key, String value) {
        return setGet(commandTimeoutMillis, key, value);
    }

    public String setGet(long commandTimeoutMillis, String key, String value) {
        return complete(commandTimeoutMillis, Calls.setGet(key, value));
    }

    public String setGet(String key, String value, SetGetOptions options) {
        return setGet(commandTimeoutMillis, key, value, options);
    }

    public String setGet(long commandTimeoutMillis, String key, String value, SetGetOptions options) {
        return complete(commandTimeoutMillis, Calls.setGet(key, value, options));
    }

    public String get(String key) {
        return get(commandTimeoutMillis, key);
    }

    public String get(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.get(key));
    }

    public long strlen(String key) {
        return strlen(commandTimeoutMillis, key);
    }

    public long strlen(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.strlen(key));
    }

    public long append(String key, String value) {
        return append(commandTimeoutMillis, key, value);
    }

    public long append(long commandTimeoutMillis, String key, String value) {
        return complete(commandTimeoutMillis, Calls.append(key, value));
    }

    public long setbit(String key, long offset, long value) {
        return setbit(commandTimeoutMillis, key, offset, value);
    }

    public long setbit(long commandTimeoutMillis, String key, long offset, long value) {
        return complete(commandTimeoutMillis, Calls.setbit(key, offset, value));
    }

    public long getbit(String key, long offset) {
        return getbit(commandTimeoutMillis, key, offset);
    }

    public long getbit(long commandTimeoutMillis, String key, long offset) {
        return complete(commandTimeoutMillis, Calls.getbit(key, offset));
    }

    public long bitcount(String key) {
        return bitcount(commandTimeoutMillis, key);
    }

    public long bitcount(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.bitcount(key));
    }

    public long bitcount(String key, long start, long end) {
        return bitcount(commandTimeoutMillis, key, start, end);
    }

    public long bitcount(long commandTimeoutMillis, String key, long start, long end) {
        return complete(commandTimeoutMillis, Calls.bitcount(key, start, end));
    }

    public long incr(String key) {
        return incr(commandTimeoutMillis, key);
    }

    public long incr(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.incr(key));
    }

    public long decr(String key) {
        return decr(commandTimeoutMillis, key);
    }

    public long decr(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.decr(key));
    }

    public long hset(String key, String field, String value, String... more) {
        return hset(commandTimeoutMillis, key, field, value, more);
    }

    public long hset(long commandTimeoutMillis, String key, String field, String value, String... more) {
        return complete(commandTimeoutMillis, Calls.hset(key, field, value, more));
    }

    public String hget(String key, String field) {
        return hget(commandTimeoutMillis, key, field);
    }

    public String hget(long commandTimeoutMillis, String key, String field) {
        return complete(commandTimeoutMillis, Calls.hget(key, field));
    }

    /**
     * 只在这个方法里把扁平数组成 {@code Map}。原始 {@code command("HGETALL")} 仍返回 {@code List}。
     */
    public Map<String, String> hgetall(String key) {
        return hgetall(commandTimeoutMillis, key);
    }

    public Map<String, String> hgetall(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.hgetall(key));
    }

    public long hlen(String key) {
        return hlen(commandTimeoutMillis, key);
    }

    public long hlen(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.hlen(key));
    }

    public long hdel(String key, String field, String... more) {
        return hdel(commandTimeoutMillis, key, field, more);
    }

    public long hdel(long commandTimeoutMillis, String key, String field, String... more) {
        return complete(commandTimeoutMillis, Calls.hdel(key, field, more));
    }

    public HashScan hscan(String key, String cursor) {
        return hscan(commandTimeoutMillis, key, cursor);
    }

    public HashScan hscan(long commandTimeoutMillis, String key, String cursor) {
        return complete(commandTimeoutMillis, Calls.hscan(key, cursor));
    }

    public HashScan hscan(String key, String cursor, ScanOptions options) {
        return hscan(commandTimeoutMillis, key, cursor, options);
    }

    public HashScan hscan(long commandTimeoutMillis, String key, String cursor, ScanOptions options) {
        return complete(commandTimeoutMillis, Calls.hscan(key, cursor, options));
    }

    /**
     * {@code HSCAN NOVALUES}。回复里只有 field，没有 value。
     */
    public HashFieldScan hscanNoValues(String key, String cursor) {
        return hscanNoValues(commandTimeoutMillis, key, cursor);
    }

    public HashFieldScan hscanNoValues(long commandTimeoutMillis, String key, String cursor) {
        return complete(commandTimeoutMillis, Calls.hscanNoValues(key, cursor));
    }

    public HashFieldScan hscanNoValues(String key, String cursor, ScanOptions options) {
        return hscanNoValues(commandTimeoutMillis, key, cursor, options);
    }

    public HashFieldScan hscanNoValues(long commandTimeoutMillis, String key, String cursor, ScanOptions options) {
        return complete(commandTimeoutMillis, Calls.hscanNoValues(key, cursor, options));
    }

    public long lpush(String key, String value, String... more) {
        return lpush(commandTimeoutMillis, key, value, more);
    }

    public long lpush(long commandTimeoutMillis, String key, String value, String... more) {
        return complete(commandTimeoutMillis, Calls.lpush(key, value, more));
    }

    public long rpush(String key, String value, String... more) {
        return rpush(commandTimeoutMillis, key, value, more);
    }

    public long rpush(long commandTimeoutMillis, String key, String value, String... more) {
        return complete(commandTimeoutMillis, Calls.rpush(key, value, more));
    }

    public List<String> lrange(String key, long start, long stop) {
        return lrange(commandTimeoutMillis, key, start, stop);
    }

    public List<String> lrange(long commandTimeoutMillis, String key, long start, long stop) {
        return complete(commandTimeoutMillis, Calls.lrange(key, start, stop));
    }

    public String lpop(String key) {
        return lpop(commandTimeoutMillis, key);
    }

    public String lpop(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.lpop(key));
    }

    /**
     * 带 count 的 {@code LPOP}。服务端只在有 count 参数时回复数组；没有元素时是 null。
     */
    public List<String> lpop(String key, long count) {
        return lpop(commandTimeoutMillis, key, count);
    }

    public List<String> lpop(long commandTimeoutMillis, String key, long count) {
        return complete(commandTimeoutMillis, Calls.lpop(key, count));
    }

    public String rpop(String key) {
        return rpop(commandTimeoutMillis, key);
    }

    public String rpop(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.rpop(key));
    }

    /**
     * 带 count 的 {@code RPOP}。服务端只在有 count 参数时回复数组；没有元素时是 null。
     */
    public List<String> rpop(String key, long count) {
        return rpop(commandTimeoutMillis, key, count);
    }

    public List<String> rpop(long commandTimeoutMillis, String key, long count) {
        return complete(commandTimeoutMillis, Calls.rpop(key, count));
    }

    public long sadd(String key, String member, String... more) {
        return sadd(commandTimeoutMillis, key, member, more);
    }

    public long sadd(long commandTimeoutMillis, String key, String member, String... more) {
        return complete(commandTimeoutMillis, Calls.sadd(key, member, more));
    }

    public long srem(String key, String member, String... more) {
        return srem(commandTimeoutMillis, key, member, more);
    }

    public long srem(long commandTimeoutMillis, String key, String member, String... more) {
        return complete(commandTimeoutMillis, Calls.srem(key, member, more));
    }

    public Set<String> smembers(String key) {
        return smembers(commandTimeoutMillis, key);
    }

    public Set<String> smembers(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.smembers(key));
    }

    public long sismember(String key, String member) {
        return sismember(commandTimeoutMillis, key, member);
    }

    public long sismember(long commandTimeoutMillis, String key, String member) {
        return complete(commandTimeoutMillis, Calls.sismember(key, member));
    }

    public long scard(String key) {
        return scard(commandTimeoutMillis, key);
    }

    public long scard(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.scard(key));
    }

    public SetScan sscan(String key, String cursor) {
        return sscan(commandTimeoutMillis, key, cursor);
    }

    public SetScan sscan(long commandTimeoutMillis, String key, String cursor) {
        return complete(commandTimeoutMillis, Calls.sscan(key, cursor));
    }

    public SetScan sscan(String key, String cursor, ScanOptions options) {
        return sscan(commandTimeoutMillis, key, cursor, options);
    }

    public SetScan sscan(long commandTimeoutMillis, String key, String cursor, ScanOptions options) {
        return complete(commandTimeoutMillis, Calls.sscan(key, cursor, options));
    }

    public long zadd(String key, String score, String member, String... more) {
        return zadd(commandTimeoutMillis, key, score, member, more);
    }

    public long zadd(long commandTimeoutMillis, String key, String score, String member, String... more) {
        return complete(commandTimeoutMillis, Calls.zadd(key, score, member, more));
    }

    public long zadd(String key, ZAddOptions options, String score, String member, String... more) {
        return zadd(commandTimeoutMillis, key, options, score, member, more);
    }

    public long zadd(long commandTimeoutMillis, String key, ZAddOptions options, String score, String member, String... more) {
        return complete(commandTimeoutMillis, Calls.zadd(key, options, score, member, more));
    }

    /**
     * {@code ZADD INCR}。回复是新分数的文本，或条件没写上时的 null。
     */
    public String zaddIncr(String key, String score, String member) {
        return zaddIncr(commandTimeoutMillis, key, score, member);
    }

    public String zaddIncr(long commandTimeoutMillis, String key, String score, String member) {
        return complete(commandTimeoutMillis, Calls.zaddIncr(key, score, member));
    }

    public String zaddIncr(String key, ZAddIncrOptions options, String score, String member) {
        return zaddIncr(commandTimeoutMillis, key, options, score, member);
    }

    public String zaddIncr(long commandTimeoutMillis, String key, ZAddIncrOptions options, String score, String member) {
        return complete(commandTimeoutMillis, Calls.zaddIncr(key, options, score, member));
    }

    public List<String> zrange(String key, long start, long stop) {
        return zrange(commandTimeoutMillis, key, start, stop);
    }

    public List<String> zrange(long commandTimeoutMillis, String key, long start, long stop) {
        return complete(commandTimeoutMillis, Calls.zrange(key, start, stop));
    }

    public List<String> zrange(String key, long start, long stop, ZRangeOptions options) {
        return zrange(commandTimeoutMillis, key, start, stop, options);
    }

    public List<String> zrange(long commandTimeoutMillis, String key, long start, long stop, ZRangeOptions options) {
        return complete(commandTimeoutMillis, Calls.zrange(key, start, stop, options));
    }

    /**
     * {@code ZRANGE WITHSCORES}。成员顺序与回复一致，分数保持文本。
     */
    public List<ScoredMember> zrangeWithScores(String key, long start, long stop) {
        return zrangeWithScores(commandTimeoutMillis, key, start, stop);
    }

    public List<ScoredMember> zrangeWithScores(long commandTimeoutMillis, String key, long start, long stop) {
        return complete(commandTimeoutMillis, Calls.zrangeWithScores(key, start, stop));
    }

    public List<ScoredMember> zrangeWithScores(String key, long start, long stop, ZRangeOptions options) {
        return zrangeWithScores(commandTimeoutMillis, key, start, stop, options);
    }

    public List<ScoredMember> zrangeWithScores(
            long commandTimeoutMillis,
            String key,
            long start,
            long stop,
            ZRangeOptions options
    ) {
        return complete(commandTimeoutMillis, Calls.zrangeWithScores(key, start, stop, options));
    }

    public List<String> zrevrange(String key, long start, long stop) {
        return zrevrange(commandTimeoutMillis, key, start, stop);
    }

    public List<String> zrevrange(long commandTimeoutMillis, String key, long start, long stop) {
        return complete(commandTimeoutMillis, Calls.zrevrange(key, start, stop));
    }

    public List<ScoredMember> zrevrangeWithScores(String key, long start, long stop) {
        return zrevrangeWithScores(commandTimeoutMillis, key, start, stop);
    }

    public List<ScoredMember> zrevrangeWithScores(long commandTimeoutMillis, String key, long start, long stop) {
        return complete(commandTimeoutMillis, Calls.zrevrangeWithScores(key, start, stop));
    }

    public List<String> zrangeByScore(String key, String min, String max) {
        return zrangeByScore(commandTimeoutMillis, key, min, max);
    }

    public List<String> zrangeByScore(long commandTimeoutMillis, String key, String min, String max) {
        return complete(commandTimeoutMillis, Calls.zrangeByScore(key, min, max));
    }

    public List<String> zrangeByScore(String key, String min, String max, ScoreRangeOptions options) {
        return zrangeByScore(commandTimeoutMillis, key, min, max, options);
    }

    public List<String> zrangeByScore(
            long commandTimeoutMillis,
            String key,
            String min,
            String max,
            ScoreRangeOptions options
    ) {
        return complete(commandTimeoutMillis, Calls.zrangeByScore(key, min, max, options));
    }

    public List<ScoredMember> zrangeByScoreWithScores(String key, String min, String max) {
        return zrangeByScoreWithScores(commandTimeoutMillis, key, min, max);
    }

    public List<ScoredMember> zrangeByScoreWithScores(long commandTimeoutMillis, String key, String min, String max) {
        return complete(commandTimeoutMillis, Calls.zrangeByScoreWithScores(key, min, max));
    }

    public List<ScoredMember> zrangeByScoreWithScores(String key, String min, String max, ScoreRangeOptions options) {
        return zrangeByScoreWithScores(commandTimeoutMillis, key, min, max, options);
    }

    public List<ScoredMember> zrangeByScoreWithScores(
            long commandTimeoutMillis,
            String key,
            String min,
            String max,
            ScoreRangeOptions options
    ) {
        return complete(commandTimeoutMillis, Calls.zrangeByScoreWithScores(key, min, max, options));
    }

    /**
     * {@code ZREVRANGEBYSCORE}。参数顺序是命令本身的 max、min。
     */
    public List<String> zrevrangeByScore(String key, String max, String min) {
        return zrevrangeByScore(commandTimeoutMillis, key, max, min);
    }

    public List<String> zrevrangeByScore(long commandTimeoutMillis, String key, String max, String min) {
        return complete(commandTimeoutMillis, Calls.zrevrangeByScore(key, max, min));
    }

    public List<String> zrevrangeByScore(String key, String max, String min, ScoreRangeOptions options) {
        return zrevrangeByScore(commandTimeoutMillis, key, max, min, options);
    }

    public List<String> zrevrangeByScore(
            long commandTimeoutMillis,
            String key,
            String max,
            String min,
            ScoreRangeOptions options
    ) {
        return complete(commandTimeoutMillis, Calls.zrevrangeByScore(key, max, min, options));
    }

    public List<ScoredMember> zrevrangeByScoreWithScores(String key, String max, String min) {
        return zrevrangeByScoreWithScores(commandTimeoutMillis, key, max, min);
    }

    public List<ScoredMember> zrevrangeByScoreWithScores(long commandTimeoutMillis, String key, String max, String min) {
        return complete(commandTimeoutMillis, Calls.zrevrangeByScoreWithScores(key, max, min));
    }

    public List<ScoredMember> zrevrangeByScoreWithScores(
            String key,
            String max,
            String min,
            ScoreRangeOptions options
    ) {
        return zrevrangeByScoreWithScores(commandTimeoutMillis, key, max, min, options);
    }

    public List<ScoredMember> zrevrangeByScoreWithScores(
            long commandTimeoutMillis,
            String key,
            String max,
            String min,
            ScoreRangeOptions options
    ) {
        return complete(commandTimeoutMillis, Calls.zrevrangeByScoreWithScores(key, max, min, options));
    }

    public long zremrangeByScore(String key, String min, String max) {
        return zremrangeByScore(commandTimeoutMillis, key, min, max);
    }

    public long zremrangeByScore(long commandTimeoutMillis, String key, String min, String max) {
        return complete(commandTimeoutMillis, Calls.zremrangeByScore(key, min, max));
    }

    public long zremrangeByRank(String key, long start, long stop) {
        return zremrangeByRank(commandTimeoutMillis, key, start, stop);
    }

    public long zremrangeByRank(long commandTimeoutMillis, String key, long start, long stop) {
        return complete(commandTimeoutMillis, Calls.zremrangeByRank(key, start, stop));
    }

    public long zrem(String key, String member, String... more) {
        return zrem(commandTimeoutMillis, key, member, more);
    }

    public long zrem(long commandTimeoutMillis, String key, String member, String... more) {
        return complete(commandTimeoutMillis, Calls.zrem(key, member, more));
    }

    public ZScan zscan(String key, String cursor) {
        return zscan(commandTimeoutMillis, key, cursor);
    }

    public ZScan zscan(long commandTimeoutMillis, String key, String cursor) {
        return complete(commandTimeoutMillis, Calls.zscan(key, cursor));
    }

    public ZScan zscan(String key, String cursor, ScanOptions options) {
        return zscan(commandTimeoutMillis, key, cursor, options);
    }

    public ZScan zscan(long commandTimeoutMillis, String key, String cursor, ScanOptions options) {
        return complete(commandTimeoutMillis, Calls.zscan(key, cursor, options));
    }

    public String type(String key) {
        return type(commandTimeoutMillis, key);
    }

    public String type(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.type(key));
    }

    public Long memoryUsage(String key) {
        return memoryUsage(commandTimeoutMillis, key);
    }

    public Long memoryUsage(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.memoryUsage(key));
    }

    public Long memoryUsage(String key, MemoryUsageOptions options) {
        return memoryUsage(commandTimeoutMillis, key, options);
    }

    public Long memoryUsage(long commandTimeoutMillis, String key, MemoryUsageOptions options) {
        return complete(commandTimeoutMillis, Calls.memoryUsage(key, options));
    }

    public Map<String, Long> memoryStats() {
        return memoryStats(commandTimeoutMillis);
    }

    public Map<String, Long> memoryStats(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.memoryStats());
    }

    public String objectEncoding(String key) {
        return objectEncoding(commandTimeoutMillis, key);
    }

    public String objectEncoding(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.objectEncoding(key));
    }

    public List<String> keys(String pattern) {
        return keys(commandTimeoutMillis, pattern);
    }

    public List<String> keys(long commandTimeoutMillis, String pattern) {
        return complete(commandTimeoutMillis, Calls.keys(pattern));
    }

    public KeyScan scan(String cursor) {
        return scan(commandTimeoutMillis, cursor);
    }

    public KeyScan scan(long commandTimeoutMillis, String cursor) {
        return complete(commandTimeoutMillis, Calls.scan(cursor));
    }

    public KeyScan scan(String cursor, ScanOptions options) {
        return scan(commandTimeoutMillis, cursor, options);
    }

    public KeyScan scan(long commandTimeoutMillis, String cursor, ScanOptions options) {
        return complete(commandTimeoutMillis, Calls.scan(cursor, options));
    }

    public long del(String... keys) {
        return del(commandTimeoutMillis, keys);
    }

    public long del(long commandTimeoutMillis, String... keys) {
        return complete(commandTimeoutMillis, Calls.del(keys));
    }

    public long exists(String... keys) {
        return exists(commandTimeoutMillis, keys);
    }

    public long exists(long commandTimeoutMillis, String... keys) {
        return complete(commandTimeoutMillis, Calls.exists(keys));
    }

    public long expire(String key, long seconds) {
        return expire(commandTimeoutMillis, key, seconds);
    }

    public long expire(long commandTimeoutMillis, String key, long seconds) {
        return complete(commandTimeoutMillis, Calls.expire(key, seconds));
    }

    public long expire(String key, long seconds, ExpireOptions options) {
        return expire(commandTimeoutMillis, key, seconds, options);
    }

    public long expire(long commandTimeoutMillis, String key, long seconds, ExpireOptions options) {
        return complete(commandTimeoutMillis, Calls.expire(key, seconds, options));
    }

    public long pexpire(String key, long milliseconds) {
        return pexpire(commandTimeoutMillis, key, milliseconds);
    }

    public long pexpire(long commandTimeoutMillis, String key, long milliseconds) {
        return complete(commandTimeoutMillis, Calls.pexpire(key, milliseconds));
    }

    public long pexpire(String key, long milliseconds, ExpireOptions options) {
        return pexpire(commandTimeoutMillis, key, milliseconds, options);
    }

    public long pexpire(long commandTimeoutMillis, String key, long milliseconds, ExpireOptions options) {
        return complete(commandTimeoutMillis, Calls.pexpire(key, milliseconds, options));
    }

    public long expireat(String key, long unixSeconds) {
        return expireat(commandTimeoutMillis, key, unixSeconds);
    }

    public long expireat(long commandTimeoutMillis, String key, long unixSeconds) {
        return complete(commandTimeoutMillis, Calls.expireat(key, unixSeconds));
    }

    public long expireat(String key, long unixSeconds, ExpireOptions options) {
        return expireat(commandTimeoutMillis, key, unixSeconds, options);
    }

    public long expireat(long commandTimeoutMillis, String key, long unixSeconds, ExpireOptions options) {
        return complete(commandTimeoutMillis, Calls.expireat(key, unixSeconds, options));
    }

    public long pexpireat(String key, long unixMilliseconds) {
        return pexpireat(commandTimeoutMillis, key, unixMilliseconds);
    }

    public long pexpireat(long commandTimeoutMillis, String key, long unixMilliseconds) {
        return complete(commandTimeoutMillis, Calls.pexpireat(key, unixMilliseconds));
    }

    public long pexpireat(String key, long unixMilliseconds, ExpireOptions options) {
        return pexpireat(commandTimeoutMillis, key, unixMilliseconds, options);
    }

    public long pexpireat(long commandTimeoutMillis, String key, long unixMilliseconds, ExpireOptions options) {
        return complete(commandTimeoutMillis, Calls.pexpireat(key, unixMilliseconds, options));
    }

    public long persist(String key) {
        return persist(commandTimeoutMillis, key);
    }

    public long persist(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.persist(key));
    }

    public long ttl(String key) {
        return ttl(commandTimeoutMillis, key);
    }

    public long ttl(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.ttl(key));
    }

    public long pttl(String key) {
        return pttl(commandTimeoutMillis, key);
    }

    public long pttl(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.pttl(key));
    }

    public long pfadd(String key, String... elements) {
        return pfadd(commandTimeoutMillis, key, elements);
    }

    public long pfadd(long commandTimeoutMillis, String key, String... elements) {
        return complete(commandTimeoutMillis, Calls.pfadd(key, elements));
    }

    public long pfcount(String... keys) {
        return pfcount(commandTimeoutMillis, keys);
    }

    public long pfcount(long commandTimeoutMillis, String... keys) {
        return complete(commandTimeoutMillis, Calls.pfcount(keys));
    }

    public String pfmerge(String destination, String... sources) {
        return pfmerge(commandTimeoutMillis, destination, sources);
    }

    public String pfmerge(long commandTimeoutMillis, String destination, String... sources) {
        return complete(commandTimeoutMillis, Calls.pfmerge(destination, sources));
    }

    boolean isClosed() {
        return closed;
    }

    long commandTimeoutMillis() {
        return commandTimeoutMillis;
    }

    Pipeline currentPipeline() {
        return pipeline;
    }

    Transaction currentTransaction() {
        return transaction;
    }

    void endPipeline(Pipeline source, boolean unread) {
        if (pipeline != source) {
            return;
        }
        pipeline = null;
        if (closed) {
            return;
        }
        if (unread) {
            close();
            return;
        }
        mode = Mode.NORMAL;
    }

    void finishTransaction(Transaction source) {
        source.markFinished();
        if (transaction != source) {
            return;
        }
        transaction = null;
        if (!closed) {
            mode = Mode.NORMAL;
        }
    }

    void writeCommand(long commandTimeoutMillis, String[] args) {
        if (closed) {
            throw new IllegalStateException("connection is closed");
        }
        validateArgs(args);
        List<byte[]> encoded = encodeArgs(args);
        try {
            socket.setSoTimeout(toSocketTimeoutMillis(commandTimeoutMillis));
            RespClientCodec.writeCommand(out, encoded);
            out.flush();
        } catch (IOException e) {
            close();
            throw new ConnectionException("connection closed after a write failure", e);
        }
    }

    Object readCommandReply(long commandTimeoutMillis) {
        try {
            socket.setSoTimeout(toSocketTimeoutMillis(commandTimeoutMillis));
            return readValue();
        } catch (SocketTimeoutException e) {
            close();
            throw new CommandTimeoutException("timed out waiting for a reply", e);
        } catch (IOException e) {
            close();
            throw new ConnectionException("connection closed after a read failure", e);
        }
    }

    private <T> T complete(long commandTimeoutMillis, Call<T> call) {
        return call.decode(command(commandTimeoutMillis, call.args));
    }

    List<Object> collectExec(List<Call<?>> queued) {
        writeCommand(commandTimeoutMillis, new String[]{"EXEC"});
        RespClientCodec.RespReply reply;
        try {
            socket.setSoTimeout(toSocketTimeoutMillis(commandTimeoutMillis));
            reply = readExecReply();
        } catch (SocketTimeoutException e) {
            close();
            throw new CommandTimeoutException("timed out waiting for a reply", e);
        } catch (IOException e) {
            close();
            throw new ConnectionException("connection closed after a read failure", e);
        }
        return projectExec(queued, reply);
    }

    void noteQueuedResults(List<Call<?>> queued, List<Object> results) {
        int count = Math.min(queued.size(), results.size());
        for (int i = 0; i < count; i++) {
            Object result = results.get(i);
            if (result instanceof ServerException || result instanceof DecodeException) {
                continue;
            }
            noteSuccessfulCommand(queued.get(i).args);
        }
    }

    private RespClientCodec.RespReply readExecReply() throws IOException {
        int type = in.read();
        if (type < 0) {
            throw new IOException("unexpected EOF before RESP reply");
        }
        if (type == '%' || type == '~' || type == '_') {
            throw new IOException("RESP3 reply marker: " + (char) type);
        }
        if (type == '-') {
            throw new ServerException(decodeReplyText(readLineBody(in)));
        }
        if (type == '+') {
            String text = decodeReplyText(readLineBody(in));
            return new RespClientCodec.RespReply(
                    RespClientCodec.RespReply.Kind.SIMPLE_STRING, text, null, null, null);
        }
        in.unread(type);
        return RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
    }

    private List<Object> projectExec(List<Call<?>> queued, RespClientCodec.RespReply reply) {
        if (reply.kind() != RespClientCodec.RespReply.Kind.ARRAY || reply.values() == null) {
            throw new DecodeException("reply was " + reply.kind() + ", expected array", null);
        }
        List<RespClientCodec.RespReply> elements = reply.values();
        if (elements.size() != queued.size()) {
            throw new DecodeException(
                    "reply was array of " + elements.size() + ", expected " + queued.size(), null);
        }
        List<Object> results = new ArrayList<>(elements.size());
        for (int i = 0; i < elements.size(); i++) {
            try {
                results.add(projectExecElement(queued.get(i), elements.get(i)));
            } catch (DecodeException failure) {
                results.add(failure);
            }
        }
        return results;
    }

    private static Object projectExecElement(Call<?> call, RespClientCodec.RespReply element) {
        if (element.kind() == RespClientCodec.RespReply.Kind.ERROR) {
            return new ServerException(serverText(element));
        }
        // 公开 convert 把嵌套 ERROR 收成字符串。这里保留成 ServerException，EXEC 列表才能把单条错误和成功文本分开。
        return call.decode(convert(element, true));
    }

    private Object readValue() throws IOException {
        int type = in.read();
        if (type < 0) {
            throw new IOException("unexpected EOF before RESP reply");
        }
        // 只看顶层回复的第一个字节。`%`、`~`、`_` 表示这条连接已经离开 RESP2，关掉它，不能交给 readReply 当成成功结果。
        if (type == '%' || type == '~' || type == '_') {
            throw new IOException("RESP3 reply marker: " + (char) type);
        }
        if (type == '+' || type == '-') {
            // codec 读 simple string 时会用替换字符吞掉非法 UTF-8。顶层 `+` / `-` 在帧读完后按 REPORT 解码。
            String text = decodeReplyText(readLineBody(in));
            if (type == '-') {
                throw new ServerException(text);
            }
            return text;
        }
        // `$` 和 `*` 仍交给 codec，这样 null bulk 和 null array 保持 null，连接继续可用。
        in.unread(type);
        return convert(RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES), false);
    }

    private static byte[] readLineBody(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int previous = -1;
        while (true) {
            int current = in.read();
            if (current < 0) {
                throw new IOException("unexpected EOF before RESP line terminator");
            }
            if (previous == '\r' && current == '\n') {
                byte[] raw = buf.toByteArray();
                return Arrays.copyOf(raw, raw.length - 1);
            }
            buf.write(current);
            // 与 codec 一样，把尚未配对的 CR 算进上限。超限当读失败关掉连接，不在半行上重同步。
            if (buf.size() > RespProtocolLimits.DEFAULT_MAX_BULK_BYTES + 1) {
                throw new IOException("RESP line exceeds limit");
            }
            previous = current;
        }
    }

    private static Object convert(RespClientCodec.RespReply reply, boolean preserveErrors) {
        return switch (reply.kind()) {
            case SIMPLE_STRING -> reply.text();
            case BULK_STRING -> {
                byte[] bytes = reply.bytes();
                yield bytes == null ? null : decodeReplyText(bytes);
            }
            case INTEGER -> {
                Long integer = reply.integer();
                if (integer == null) {
                    throw new DecodeException("integer reply has no value", null);
                }
                yield integer;
            }
            case NULL -> null;
            // 顶层 `-` 已在 readValue 里抛出。公开原始命令把数组元素里的错误收成文本。
            // EXEC 不走这条路径，否则单条错误会变成普通字符串。
            case ERROR -> preserveErrors ? new ServerException(serverText(reply)) : reply.text();
            case ARRAY, MAP, SET -> convertAggregate(reply.values(), preserveErrors);
        };
    }

    private static List<Object> convertAggregate(List<RespClientCodec.RespReply> values, boolean preserveErrors) {
        if (values == null) {
            throw new DecodeException("aggregate reply has no elements", null);
        }
        List<Object> converted = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            converted.add(convert(values.get(i), preserveErrors));
        }
        return converted;
    }

    private static String serverText(RespClientCodec.RespReply reply) {
        String text = reply.text();
        if (text == null) {
            throw new DecodeException("error reply has no text", null);
        }
        return text;
    }

    void noteSuccessfulCommand(String[] args) {
        if (isCommand(args[0], "QUIT")) {
            // QUIT 的回复先返回给调用方，本地再关掉。服务端错误在读回复时已经抛出，不会走到这里。
            close();
            return;
        }
        if (isCommand(args[0], "SELECT") && args.length >= 2) {
            Integer index = parseDbIndex(args[1]);
            if (index != null) {
                database = index;
            }
        }
    }

    private static void validateArgs(String[] args) {
        if (args == null || args.length == 0 || args[0] == null || args[0].isEmpty()) {
            throw new IllegalArgumentException("command is required");
        }
        for (int i = 1; i < args.length; i++) {
            if (args[i] == null) {
                throw new IllegalArgumentException("command argument is null");
            }
        }
    }

    private static List<byte[]> encodeArgs(String[] args) {
        // 全部参数先按 UTF-8 REPORT 编完，再写第一个字节。某个参数编不成时，连接上还没有任何半条命令。
        CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        List<byte[]> encoded = new ArrayList<>(args.length);
        for (String arg : args) {
            ByteBuffer buffer;
            try {
                buffer = encoder.encode(CharBuffer.wrap(arg));
            } catch (CharacterCodingException e) {
                throw new DecodeException("command argument cannot be encoded as UTF-8", e);
            }
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            encoded.add(bytes);
        }
        return encoded;
    }

    private static String decodeReplyText(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new DecodeException("reply text is not valid UTF-8", e);
        }
    }

    private static boolean isCommand(String name, String expected) {
        return name.equalsIgnoreCase(expected);
    }

    private static Integer parseDbIndex(String text) {
        try {
            long value = Long.parseLong(text);
            if (value < 0 || value > Integer.MAX_VALUE) {
                return null;
            }
            return (int) value;
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static int toSocketTimeoutMillis(long timeoutMillis) {
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeout must be > 0");
        }
        return timeoutMillis > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) timeoutMillis;
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }
}
