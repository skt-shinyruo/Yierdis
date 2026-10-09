package yier.bubu.redis.client;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.ScheduledFuture;
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
import yier.bubu.redis.client.exception.CommandTimeoutException;
import yier.bubu.redis.client.exception.ConnectionException;
import yier.bubu.redis.client.exception.DecodeException;
import yier.bubu.redis.client.exception.ServerException;
import yier.bubu.redis.client.internal.Call;
import yier.bubu.redis.client.internal.Calls;
import yier.bubu.redis.client.internal.CommandChannel;
import yier.bubu.redis.client.reply.CommandInfo;
import yier.bubu.redis.client.reply.HashFieldScan;
import yier.bubu.redis.client.reply.HashScan;
import yier.bubu.redis.client.reply.KeyScan;
import yier.bubu.redis.client.reply.ScoredMember;
import yier.bubu.redis.client.reply.SetScan;
import yier.bubu.redis.client.reply.ZScan;
import yier.bubu.redis.protocol.resp.RespClientCodec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

/**
 * 指向一台 standalone Yierdis 的异步连接。同一时刻只由一个调用方使用。
 * <p>
 * 提交在事件循环接受写出之后返回 {@link CompletableFuture}。接受之前的失败在调用方线程抛出，没有 Future。
 * Future 在这条连接的事件循环上完成。调用方要结果就在自己的线程上等待。
 * 原始命令完成成 {@code String}、{@code Long}、{@code null} 或嵌套 {@code List}。
 * 类型化方法在此之上检查回复形状；形状不对只失败这一次，回复已经读完，连接可以继续用。
 * 带 {@code commandTimeoutMillis} 的重载只作用于这一次提交，从写出开始计时。
 * {@code HELLO} 没有类型化方法。{@link #multi()} 进入事务；{@code EXEC} 和 {@code DISCARD} 在事务对象上。
 * 原始 {@code MULTI} 同样在提交时进入事务，之后这条连接的原始命令入队并结束事务，类型化命令仍在写出前拒绝。
 * <p>
 * 服务端错误和 UTF-8 解码失败都读完当前回复，连接可以继续用。
 * 超时只失败这一条，它继续占着队列，晚到的回复由它吃掉。
 * 读写失败、连接断开、帧边界对不上、超过 512 MiB 的 bulk 会关掉连接，并且不自动重连。
 */
public final class Connection implements AutoCloseable {
    private enum Mode {
        NORMAL,
        TRANSACTION
    }

    private enum PendingKind {
        NORMAL,
        MULTI,
        QUEUED,
        EXEC,
        DISCARD
    }

    private final CommandChannel channel;
    private final long commandTimeoutMillis;
    private final ArrayDeque<Pending> pending = new ArrayDeque<>();
    private final List<Pending> transactionBatch = new ArrayList<>();
    // 同一条连接只处在普通或事务。未配对命令不是另一种模式。
    private volatile Mode mode = Mode.NORMAL;
    private Transaction transaction;
    private boolean rawTransaction;
    private volatile int database;
    private volatile int pendingCount;
    private volatile boolean closed;

    private Connection(CommandChannel channel, long commandTimeoutMillis) {
        this.channel = channel;
        this.commandTimeoutMillis = commandTimeoutMillis;
    }

    public static Connection connect() {
        return connect(ConnectionSettings.defaults());
    }

    public static Connection connect(String host, int port) {
        return connect(ConnectionSettings.defaults().withHost(host).withPort(port));
    }

    public static Connection connect(ConnectionSettings settings) {
        Objects.requireNonNull(settings, "settings");
        return connect(settings, null, true, settings.connectTimeoutMillis(), settings.commandTimeoutMillis());
    }

    // 这两个超时只作用于这次打开。连接保存的命令超时仍是 settings 里的值。
    // 池的借出传入自己的事件循环，TCP 返回后还要按当时剩余的借出等待决定 SELECT 超时。
    static Connection connect(
            ConnectionSettings settings,
            EventLoopGroup group,
            boolean ownsGroup,
            long socketTimeoutMillis,
            long setupCommandTimeoutMillis
    ) {
        Connection[] self = new Connection[1];
        CommandChannel channel = CommandChannel.open(settings, group, ownsGroup, socketTimeoutMillis, new CommandChannel.Listener() {
            @Override
            public void onReply(RespClientCodec.RespReply reply) {
                self[0].onReply(reply);
            }

            @Override
            public void onTransportFailure(Throwable cause) {
                self[0].onTransportFailure(cause);
            }
        });
        Connection connection = new Connection(channel, settings.commandTimeoutMillis());
        self[0] = connection;
        try {
            if (settings.database() != 0) {
                // 非 0 的 DB 要在把连接交给调用方之前 SELECT 成功。失败时关掉通道，调用方拿不到这条连接。
                await(connection.command(setupCommandTimeoutMillis, "SELECT", Integer.toString(settings.database())));
            }
            return connection;
        } catch (Throwable failure) {
            connection.close();
            throw failure;
        }
    }

    public CompletableFuture<Object> command(String... args) {
        return command(commandTimeoutMillis, args);
    }

    public CompletableFuture<Object> command(long commandTimeoutMillis, String... args) {
        if (closed) {
            throw new IllegalStateException("connection is closed");
        }
        if (commandTimeoutMillis <= 0) {
            throw new IllegalArgumentException("commandTimeoutMillis must be > 0");
        }
        if (mode == Mode.TRANSACTION && rawTransaction) {
            return transaction.command(commandTimeoutMillis, args);
        }
        // 对象事务使用事务对象。普通提交在事务模式写出前拒绝。
        if (mode != Mode.NORMAL) {
            throw new IllegalStateException("connection is not in normal mode");
        }
        validateArgs(args);
        if (isCommand(args[0], "MULTI")) {
            return submit(commandTimeoutMillis, Call.raw(args), PendingKind.MULTI, () -> beginRawTransaction(), null);
        }
        return submit(commandTimeoutMillis, Call.raw(args), PendingKind.NORMAL, null, null);
    }

    /**
     * 没有未配对命令时提交 {@code MULTI}，连接立刻进入事务并返回事务对象，不等 {@code OK}。
     * 已经在事务里时在写出前拒绝。{@code MULTI} 的回复如果不是 {@code OK}，这条连接会被关掉。
     */
    public Transaction multi() {
        if (closed) {
            throw new IllegalStateException("connection is closed");
        }
        Transaction[] opened = new Transaction[1];
        submit(commandTimeoutMillis, Call.raw("MULTI"), PendingKind.MULTI, () -> {
            if (mode != Mode.NORMAL || !pending.isEmpty()) {
                throw new IllegalStateException(mode != Mode.NORMAL
                        ? "connection is not in normal mode"
                        : "connection has unpaired commands");
            }
            opened[0] = new Transaction(this);
            mode = Mode.TRANSACTION;
            transaction = opened[0];
            rawTransaction = false;
        }, null);
        return opened[0];
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
        Runnable closeTask = () -> {
            if (closed) {
                return;
            }
            closed = true;
            // 调用方放弃连接时不补 EXEC 或 DISCARD。未配对命令全部失败。
            failPending(new ConnectionException("connection is closed", null));
            mode = Mode.NORMAL;
            if (transaction != null) {
                transaction.markFinished();
                transaction = null;
            }
            rawTransaction = false;
            channel.closeChannel();
        };
        if (channel.eventLoop().inEventLoop()) {
            closeTask.run();
            channel.shutdownOwnedGroup();
            return;
        }
        channel.eventLoop().submit(closeTask).syncUninterruptibly();
        channel.shutdownOwnedGroup();
    }

    public CompletableFuture<String> ping() {
        return ping(commandTimeoutMillis);
    }

    public CompletableFuture<String> ping(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.ping());
    }

    public CompletableFuture<String> ping(String message) {
        return ping(commandTimeoutMillis, message);
    }

    public CompletableFuture<String> ping(long commandTimeoutMillis, String message) {
        return complete(commandTimeoutMillis, Calls.ping(message));
    }

    public CompletableFuture<String> echo(String message) {
        return echo(commandTimeoutMillis, message);
    }

    public CompletableFuture<String> echo(long commandTimeoutMillis, String message) {
        return complete(commandTimeoutMillis, Calls.echo(message));
    }

    public CompletableFuture<List<CommandInfo>> commandList() {
        return commandList(commandTimeoutMillis);
    }

    public CompletableFuture<List<CommandInfo>> commandList(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.commandList());
    }

    public CompletableFuture<Long> commandCount() {
        return commandCount(commandTimeoutMillis);
    }

    public CompletableFuture<Long> commandCount(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.commandCount());
    }

    public CompletableFuture<List<CommandInfo>> commandInfo(String... names) {
        return commandInfo(commandTimeoutMillis, names);
    }

    public CompletableFuture<List<CommandInfo>> commandInfo(long commandTimeoutMillis, String... names) {
        return complete(commandTimeoutMillis, Calls.commandInfo(names));
    }

    /**
     * 走原始 {@code SELECT}。成功回复返回之后，{@link #database()} 才更新。
     */
    public CompletableFuture<String> select(int index) {
        return select(commandTimeoutMillis, index);
    }

    public CompletableFuture<String> select(long commandTimeoutMillis, int index) {
        return complete(commandTimeoutMillis, Calls.select(index));
    }

    public CompletableFuture<String> quit() {
        return quit(commandTimeoutMillis);
    }

    public CompletableFuture<String> quit(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.quit());
    }

    public CompletableFuture<String> clientSetname(String name) {
        return clientSetname(commandTimeoutMillis, name);
    }

    public CompletableFuture<String> clientSetname(long commandTimeoutMillis, String name) {
        return complete(commandTimeoutMillis, Calls.clientSetname(name));
    }

    public CompletableFuture<String> clientGetname() {
        return clientGetname(commandTimeoutMillis);
    }

    public CompletableFuture<String> clientGetname(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.clientGetname());
    }

    public CompletableFuture<String> clientSetinfo(String attribute, String value) {
        return clientSetinfo(commandTimeoutMillis, attribute, value);
    }

    public CompletableFuture<String> clientSetinfo(long commandTimeoutMillis, String attribute, String value) {
        return complete(commandTimeoutMillis, Calls.clientSetinfo(attribute, value));
    }

    public CompletableFuture<Object> auth(String password) {
        return auth(commandTimeoutMillis, password);
    }

    public CompletableFuture<Object> auth(long commandTimeoutMillis, String password) {
        return complete(commandTimeoutMillis, Calls.auth(password));
    }

    public CompletableFuture<Object> auth(String username, String password) {
        return auth(commandTimeoutMillis, username, password);
    }

    public CompletableFuture<Object> auth(long commandTimeoutMillis, String username, String password) {
        return complete(commandTimeoutMillis, Calls.auth(username, password));
    }

    public CompletableFuture<String> flushdb() {
        return flushdb(commandTimeoutMillis);
    }

    public CompletableFuture<String> flushdb(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.flushdb());
    }

    public CompletableFuture<String> flushdb(FlushMode mode) {
        return flushdb(commandTimeoutMillis, mode);
    }

    public CompletableFuture<String> flushdb(long commandTimeoutMillis, FlushMode mode) {
        return complete(commandTimeoutMillis, Calls.flushdb(mode));
    }

    /**
     * {@code INFO} 的 bulk 文本。单独的 health / yierdis 节不是这段文本。
     */
    public CompletableFuture<String> info() {
        return info(commandTimeoutMillis);
    }

    public CompletableFuture<String> info(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.info());
    }

    public CompletableFuture<String> info(String... sections) {
        return info(commandTimeoutMillis, sections);
    }

    public CompletableFuture<String> info(long commandTimeoutMillis, String... sections) {
        return complete(commandTimeoutMillis, Calls.info(sections));
    }

    /**
     * 单独的 {@code INFO health}。这一节是字段数组，不是 {@link #info()} 的 bulk 文本。
     */
    public CompletableFuture<Map<String, Object>> infoHealth() {
        return infoHealth(commandTimeoutMillis);
    }

    public CompletableFuture<Map<String, Object>> infoHealth(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.infoHealth());
    }

    /**
     * 单独的 {@code INFO yierdis}。这一节是字段数组，不是 {@link #info()} 的 bulk 文本。
     */
    public CompletableFuture<Map<String, Object>> infoYierdis() {
        return infoYierdis(commandTimeoutMillis);
    }

    public CompletableFuture<Map<String, Object>> infoYierdis(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.infoYierdis());
    }

    /**
     * {@code STATS}。数值字段是 {@code Long}，文本字段是 {@code String}，顺序与回复一致。
     */
    public CompletableFuture<Map<String, Object>> stats() {
        return stats(commandTimeoutMillis);
    }

    public CompletableFuture<Map<String, Object>> stats(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.stats());
    }

    public CompletableFuture<String> ydreconcile() {
        return ydreconcile(commandTimeoutMillis);
    }

    public CompletableFuture<String> ydreconcile(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.ydreconcile());
    }

    public CompletableFuture<String> set(String key, String value) {
        return set(commandTimeoutMillis, key, value);
    }

    public CompletableFuture<String> set(long commandTimeoutMillis, String key, String value) {
        return complete(commandTimeoutMillis, Calls.set(key, value));
    }

    public CompletableFuture<String> set(String key, String value, SetOptions options) {
        return set(commandTimeoutMillis, key, value, options);
    }

    public CompletableFuture<String> set(long commandTimeoutMillis, String key, String value, SetOptions options) {
        return complete(commandTimeoutMillis, Calls.set(key, value, options));
    }

    /**
     * {@code SET GET}。回复是旧值，或原来没有值时的 null。
     */
    public CompletableFuture<String> setGet(String key, String value) {
        return setGet(commandTimeoutMillis, key, value);
    }

    public CompletableFuture<String> setGet(long commandTimeoutMillis, String key, String value) {
        return complete(commandTimeoutMillis, Calls.setGet(key, value));
    }

    public CompletableFuture<String> setGet(String key, String value, SetGetOptions options) {
        return setGet(commandTimeoutMillis, key, value, options);
    }

    public CompletableFuture<String> setGet(long commandTimeoutMillis, String key, String value, SetGetOptions options) {
        return complete(commandTimeoutMillis, Calls.setGet(key, value, options));
    }

    public CompletableFuture<String> get(String key) {
        return get(commandTimeoutMillis, key);
    }

    public CompletableFuture<String> get(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.get(key));
    }

    public CompletableFuture<Long> strlen(String key) {
        return strlen(commandTimeoutMillis, key);
    }

    public CompletableFuture<Long> strlen(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.strlen(key));
    }

    public CompletableFuture<Long> append(String key, String value) {
        return append(commandTimeoutMillis, key, value);
    }

    public CompletableFuture<Long> append(long commandTimeoutMillis, String key, String value) {
        return complete(commandTimeoutMillis, Calls.append(key, value));
    }

    public CompletableFuture<Long> setbit(String key, long offset, long value) {
        return setbit(commandTimeoutMillis, key, offset, value);
    }

    public CompletableFuture<Long> setbit(long commandTimeoutMillis, String key, long offset, long value) {
        return complete(commandTimeoutMillis, Calls.setbit(key, offset, value));
    }

    public CompletableFuture<Long> getbit(String key, long offset) {
        return getbit(commandTimeoutMillis, key, offset);
    }

    public CompletableFuture<Long> getbit(long commandTimeoutMillis, String key, long offset) {
        return complete(commandTimeoutMillis, Calls.getbit(key, offset));
    }

    public CompletableFuture<Long> bitcount(String key) {
        return bitcount(commandTimeoutMillis, key);
    }

    public CompletableFuture<Long> bitcount(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.bitcount(key));
    }

    public CompletableFuture<Long> bitcount(String key, long start, long end) {
        return bitcount(commandTimeoutMillis, key, start, end);
    }

    public CompletableFuture<Long> bitcount(long commandTimeoutMillis, String key, long start, long end) {
        return complete(commandTimeoutMillis, Calls.bitcount(key, start, end));
    }

    public CompletableFuture<Long> incr(String key) {
        return incr(commandTimeoutMillis, key);
    }

    public CompletableFuture<Long> incr(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.incr(key));
    }

    public CompletableFuture<Long> decr(String key) {
        return decr(commandTimeoutMillis, key);
    }

    public CompletableFuture<Long> decr(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.decr(key));
    }

    public CompletableFuture<Long> hset(String key, String field, String value, String... more) {
        return hset(commandTimeoutMillis, key, field, value, more);
    }

    public CompletableFuture<Long> hset(long commandTimeoutMillis, String key, String field, String value, String... more) {
        return complete(commandTimeoutMillis, Calls.hset(key, field, value, more));
    }

    public CompletableFuture<String> hget(String key, String field) {
        return hget(commandTimeoutMillis, key, field);
    }

    public CompletableFuture<String> hget(long commandTimeoutMillis, String key, String field) {
        return complete(commandTimeoutMillis, Calls.hget(key, field));
    }

    /**
     * 只在这个方法里把扁平数组成 {@code Map}。原始 {@code command("HGETALL")} 仍返回 {@code List}。
     */
    public CompletableFuture<Map<String, String>> hgetall(String key) {
        return hgetall(commandTimeoutMillis, key);
    }

    public CompletableFuture<Map<String, String>> hgetall(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.hgetall(key));
    }

    public CompletableFuture<Long> hlen(String key) {
        return hlen(commandTimeoutMillis, key);
    }

    public CompletableFuture<Long> hlen(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.hlen(key));
    }

    public CompletableFuture<Long> hdel(String key, String field, String... more) {
        return hdel(commandTimeoutMillis, key, field, more);
    }

    public CompletableFuture<Long> hdel(long commandTimeoutMillis, String key, String field, String... more) {
        return complete(commandTimeoutMillis, Calls.hdel(key, field, more));
    }

    public CompletableFuture<HashScan> hscan(String key, String cursor) {
        return hscan(commandTimeoutMillis, key, cursor);
    }

    public CompletableFuture<HashScan> hscan(long commandTimeoutMillis, String key, String cursor) {
        return complete(commandTimeoutMillis, Calls.hscan(key, cursor));
    }

    public CompletableFuture<HashScan> hscan(String key, String cursor, ScanOptions options) {
        return hscan(commandTimeoutMillis, key, cursor, options);
    }

    public CompletableFuture<HashScan> hscan(long commandTimeoutMillis, String key, String cursor, ScanOptions options) {
        return complete(commandTimeoutMillis, Calls.hscan(key, cursor, options));
    }

    /**
     * {@code HSCAN NOVALUES}。回复里只有 field，没有 value。
     */
    public CompletableFuture<HashFieldScan> hscanNoValues(String key, String cursor) {
        return hscanNoValues(commandTimeoutMillis, key, cursor);
    }

    public CompletableFuture<HashFieldScan> hscanNoValues(long commandTimeoutMillis, String key, String cursor) {
        return complete(commandTimeoutMillis, Calls.hscanNoValues(key, cursor));
    }

    public CompletableFuture<HashFieldScan> hscanNoValues(String key, String cursor, ScanOptions options) {
        return hscanNoValues(commandTimeoutMillis, key, cursor, options);
    }

    public CompletableFuture<HashFieldScan> hscanNoValues(long commandTimeoutMillis, String key, String cursor, ScanOptions options) {
        return complete(commandTimeoutMillis, Calls.hscanNoValues(key, cursor, options));
    }

    public CompletableFuture<Long> lpush(String key, String value, String... more) {
        return lpush(commandTimeoutMillis, key, value, more);
    }

    public CompletableFuture<Long> lpush(long commandTimeoutMillis, String key, String value, String... more) {
        return complete(commandTimeoutMillis, Calls.lpush(key, value, more));
    }

    public CompletableFuture<Long> rpush(String key, String value, String... more) {
        return rpush(commandTimeoutMillis, key, value, more);
    }

    public CompletableFuture<Long> rpush(long commandTimeoutMillis, String key, String value, String... more) {
        return complete(commandTimeoutMillis, Calls.rpush(key, value, more));
    }

    public CompletableFuture<List<String>> lrange(String key, long start, long stop) {
        return lrange(commandTimeoutMillis, key, start, stop);
    }

    public CompletableFuture<List<String>> lrange(long commandTimeoutMillis, String key, long start, long stop) {
        return complete(commandTimeoutMillis, Calls.lrange(key, start, stop));
    }

    public CompletableFuture<String> lpop(String key) {
        return lpop(commandTimeoutMillis, key);
    }

    public CompletableFuture<String> lpop(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.lpop(key));
    }

    /**
     * 带 count 的 {@code LPOP}。服务端只在有 count 参数时回复数组；没有元素时是 null。
     */
    public CompletableFuture<List<String>> lpop(String key, long count) {
        return lpop(commandTimeoutMillis, key, count);
    }

    public CompletableFuture<List<String>> lpop(long commandTimeoutMillis, String key, long count) {
        return complete(commandTimeoutMillis, Calls.lpop(key, count));
    }

    public CompletableFuture<String> rpop(String key) {
        return rpop(commandTimeoutMillis, key);
    }

    public CompletableFuture<String> rpop(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.rpop(key));
    }

    /**
     * 带 count 的 {@code RPOP}。服务端只在有 count 参数时回复数组；没有元素时是 null。
     */
    public CompletableFuture<List<String>> rpop(String key, long count) {
        return rpop(commandTimeoutMillis, key, count);
    }

    public CompletableFuture<List<String>> rpop(long commandTimeoutMillis, String key, long count) {
        return complete(commandTimeoutMillis, Calls.rpop(key, count));
    }

    public CompletableFuture<Long> sadd(String key, String member, String... more) {
        return sadd(commandTimeoutMillis, key, member, more);
    }

    public CompletableFuture<Long> sadd(long commandTimeoutMillis, String key, String member, String... more) {
        return complete(commandTimeoutMillis, Calls.sadd(key, member, more));
    }

    public CompletableFuture<Long> srem(String key, String member, String... more) {
        return srem(commandTimeoutMillis, key, member, more);
    }

    public CompletableFuture<Long> srem(long commandTimeoutMillis, String key, String member, String... more) {
        return complete(commandTimeoutMillis, Calls.srem(key, member, more));
    }

    public CompletableFuture<Set<String>> smembers(String key) {
        return smembers(commandTimeoutMillis, key);
    }

    public CompletableFuture<Set<String>> smembers(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.smembers(key));
    }

    public CompletableFuture<Long> sismember(String key, String member) {
        return sismember(commandTimeoutMillis, key, member);
    }

    public CompletableFuture<Long> sismember(long commandTimeoutMillis, String key, String member) {
        return complete(commandTimeoutMillis, Calls.sismember(key, member));
    }

    public CompletableFuture<Long> scard(String key) {
        return scard(commandTimeoutMillis, key);
    }

    public CompletableFuture<Long> scard(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.scard(key));
    }

    public CompletableFuture<SetScan> sscan(String key, String cursor) {
        return sscan(commandTimeoutMillis, key, cursor);
    }

    public CompletableFuture<SetScan> sscan(long commandTimeoutMillis, String key, String cursor) {
        return complete(commandTimeoutMillis, Calls.sscan(key, cursor));
    }

    public CompletableFuture<SetScan> sscan(String key, String cursor, ScanOptions options) {
        return sscan(commandTimeoutMillis, key, cursor, options);
    }

    public CompletableFuture<SetScan> sscan(long commandTimeoutMillis, String key, String cursor, ScanOptions options) {
        return complete(commandTimeoutMillis, Calls.sscan(key, cursor, options));
    }

    public CompletableFuture<Long> zadd(String key, String score, String member, String... more) {
        return zadd(commandTimeoutMillis, key, score, member, more);
    }

    public CompletableFuture<Long> zadd(long commandTimeoutMillis, String key, String score, String member, String... more) {
        return complete(commandTimeoutMillis, Calls.zadd(key, score, member, more));
    }

    public CompletableFuture<Long> zadd(String key, ZAddOptions options, String score, String member, String... more) {
        return zadd(commandTimeoutMillis, key, options, score, member, more);
    }

    public CompletableFuture<Long> zadd(long commandTimeoutMillis, String key, ZAddOptions options, String score, String member, String... more) {
        return complete(commandTimeoutMillis, Calls.zadd(key, options, score, member, more));
    }

    /**
     * {@code ZADD INCR}。回复是新分数的文本，或条件没写上时的 null。
     */
    public CompletableFuture<String> zaddIncr(String key, String score, String member) {
        return zaddIncr(commandTimeoutMillis, key, score, member);
    }

    public CompletableFuture<String> zaddIncr(long commandTimeoutMillis, String key, String score, String member) {
        return complete(commandTimeoutMillis, Calls.zaddIncr(key, score, member));
    }

    public CompletableFuture<String> zaddIncr(String key, ZAddIncrOptions options, String score, String member) {
        return zaddIncr(commandTimeoutMillis, key, options, score, member);
    }

    public CompletableFuture<String> zaddIncr(long commandTimeoutMillis, String key, ZAddIncrOptions options, String score, String member) {
        return complete(commandTimeoutMillis, Calls.zaddIncr(key, options, score, member));
    }

    public CompletableFuture<List<String>> zrange(String key, long start, long stop) {
        return zrange(commandTimeoutMillis, key, start, stop);
    }

    public CompletableFuture<List<String>> zrange(long commandTimeoutMillis, String key, long start, long stop) {
        return complete(commandTimeoutMillis, Calls.zrange(key, start, stop));
    }

    public CompletableFuture<List<String>> zrange(String key, long start, long stop, ZRangeOptions options) {
        return zrange(commandTimeoutMillis, key, start, stop, options);
    }

    public CompletableFuture<List<String>> zrange(long commandTimeoutMillis, String key, long start, long stop, ZRangeOptions options) {
        return complete(commandTimeoutMillis, Calls.zrange(key, start, stop, options));
    }

    /**
     * {@code ZRANGE WITHSCORES}。成员顺序与回复一致，分数保持文本。
     */
    public CompletableFuture<List<ScoredMember>> zrangeWithScores(String key, long start, long stop) {
        return zrangeWithScores(commandTimeoutMillis, key, start, stop);
    }

    public CompletableFuture<List<ScoredMember>> zrangeWithScores(long commandTimeoutMillis, String key, long start, long stop) {
        return complete(commandTimeoutMillis, Calls.zrangeWithScores(key, start, stop));
    }

    public CompletableFuture<List<ScoredMember>> zrangeWithScores(String key, long start, long stop, ZRangeOptions options) {
        return zrangeWithScores(commandTimeoutMillis, key, start, stop, options);
    }

    public CompletableFuture<List<ScoredMember>> zrangeWithScores(
            long commandTimeoutMillis,
            String key,
            long start,
            long stop,
            ZRangeOptions options
    ) {
        return complete(commandTimeoutMillis, Calls.zrangeWithScores(key, start, stop, options));
    }

    public CompletableFuture<List<String>> zrevrange(String key, long start, long stop) {
        return zrevrange(commandTimeoutMillis, key, start, stop);
    }

    public CompletableFuture<List<String>> zrevrange(long commandTimeoutMillis, String key, long start, long stop) {
        return complete(commandTimeoutMillis, Calls.zrevrange(key, start, stop));
    }

    public CompletableFuture<List<ScoredMember>> zrevrangeWithScores(String key, long start, long stop) {
        return zrevrangeWithScores(commandTimeoutMillis, key, start, stop);
    }

    public CompletableFuture<List<ScoredMember>> zrevrangeWithScores(long commandTimeoutMillis, String key, long start, long stop) {
        return complete(commandTimeoutMillis, Calls.zrevrangeWithScores(key, start, stop));
    }

    public CompletableFuture<List<String>> zrangeByScore(String key, String min, String max) {
        return zrangeByScore(commandTimeoutMillis, key, min, max);
    }

    public CompletableFuture<List<String>> zrangeByScore(long commandTimeoutMillis, String key, String min, String max) {
        return complete(commandTimeoutMillis, Calls.zrangeByScore(key, min, max));
    }

    public CompletableFuture<List<String>> zrangeByScore(String key, String min, String max, ScoreRangeOptions options) {
        return zrangeByScore(commandTimeoutMillis, key, min, max, options);
    }

    public CompletableFuture<List<String>> zrangeByScore(
            long commandTimeoutMillis,
            String key,
            String min,
            String max,
            ScoreRangeOptions options
    ) {
        return complete(commandTimeoutMillis, Calls.zrangeByScore(key, min, max, options));
    }

    public CompletableFuture<List<ScoredMember>> zrangeByScoreWithScores(String key, String min, String max) {
        return zrangeByScoreWithScores(commandTimeoutMillis, key, min, max);
    }

    public CompletableFuture<List<ScoredMember>> zrangeByScoreWithScores(long commandTimeoutMillis, String key, String min, String max) {
        return complete(commandTimeoutMillis, Calls.zrangeByScoreWithScores(key, min, max));
    }

    public CompletableFuture<List<ScoredMember>> zrangeByScoreWithScores(String key, String min, String max, ScoreRangeOptions options) {
        return zrangeByScoreWithScores(commandTimeoutMillis, key, min, max, options);
    }

    public CompletableFuture<List<ScoredMember>> zrangeByScoreWithScores(
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
    public CompletableFuture<List<String>> zrevrangeByScore(String key, String max, String min) {
        return zrevrangeByScore(commandTimeoutMillis, key, max, min);
    }

    public CompletableFuture<List<String>> zrevrangeByScore(long commandTimeoutMillis, String key, String max, String min) {
        return complete(commandTimeoutMillis, Calls.zrevrangeByScore(key, max, min));
    }

    public CompletableFuture<List<String>> zrevrangeByScore(String key, String max, String min, ScoreRangeOptions options) {
        return zrevrangeByScore(commandTimeoutMillis, key, max, min, options);
    }

    public CompletableFuture<List<String>> zrevrangeByScore(
            long commandTimeoutMillis,
            String key,
            String max,
            String min,
            ScoreRangeOptions options
    ) {
        return complete(commandTimeoutMillis, Calls.zrevrangeByScore(key, max, min, options));
    }

    public CompletableFuture<List<ScoredMember>> zrevrangeByScoreWithScores(String key, String max, String min) {
        return zrevrangeByScoreWithScores(commandTimeoutMillis, key, max, min);
    }

    public CompletableFuture<List<ScoredMember>> zrevrangeByScoreWithScores(long commandTimeoutMillis, String key, String max, String min) {
        return complete(commandTimeoutMillis, Calls.zrevrangeByScoreWithScores(key, max, min));
    }

    public CompletableFuture<List<ScoredMember>> zrevrangeByScoreWithScores(
            String key,
            String max,
            String min,
            ScoreRangeOptions options
    ) {
        return zrevrangeByScoreWithScores(commandTimeoutMillis, key, max, min, options);
    }

    public CompletableFuture<List<ScoredMember>> zrevrangeByScoreWithScores(
            long commandTimeoutMillis,
            String key,
            String max,
            String min,
            ScoreRangeOptions options
    ) {
        return complete(commandTimeoutMillis, Calls.zrevrangeByScoreWithScores(key, max, min, options));
    }

    public CompletableFuture<Long> zremrangeByScore(String key, String min, String max) {
        return zremrangeByScore(commandTimeoutMillis, key, min, max);
    }

    public CompletableFuture<Long> zremrangeByScore(long commandTimeoutMillis, String key, String min, String max) {
        return complete(commandTimeoutMillis, Calls.zremrangeByScore(key, min, max));
    }

    public CompletableFuture<Long> zremrangeByRank(String key, long start, long stop) {
        return zremrangeByRank(commandTimeoutMillis, key, start, stop);
    }

    public CompletableFuture<Long> zremrangeByRank(long commandTimeoutMillis, String key, long start, long stop) {
        return complete(commandTimeoutMillis, Calls.zremrangeByRank(key, start, stop));
    }

    public CompletableFuture<Long> zrem(String key, String member, String... more) {
        return zrem(commandTimeoutMillis, key, member, more);
    }

    public CompletableFuture<Long> zrem(long commandTimeoutMillis, String key, String member, String... more) {
        return complete(commandTimeoutMillis, Calls.zrem(key, member, more));
    }

    public CompletableFuture<ZScan> zscan(String key, String cursor) {
        return zscan(commandTimeoutMillis, key, cursor);
    }

    public CompletableFuture<ZScan> zscan(long commandTimeoutMillis, String key, String cursor) {
        return complete(commandTimeoutMillis, Calls.zscan(key, cursor));
    }

    public CompletableFuture<ZScan> zscan(String key, String cursor, ScanOptions options) {
        return zscan(commandTimeoutMillis, key, cursor, options);
    }

    public CompletableFuture<ZScan> zscan(long commandTimeoutMillis, String key, String cursor, ScanOptions options) {
        return complete(commandTimeoutMillis, Calls.zscan(key, cursor, options));
    }

    public CompletableFuture<String> type(String key) {
        return type(commandTimeoutMillis, key);
    }

    public CompletableFuture<String> type(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.type(key));
    }

    public CompletableFuture<Long> memoryUsage(String key) {
        return memoryUsage(commandTimeoutMillis, key);
    }

    public CompletableFuture<Long> memoryUsage(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.memoryUsage(key));
    }

    public CompletableFuture<Long> memoryUsage(String key, MemoryUsageOptions options) {
        return memoryUsage(commandTimeoutMillis, key, options);
    }

    public CompletableFuture<Long> memoryUsage(long commandTimeoutMillis, String key, MemoryUsageOptions options) {
        return complete(commandTimeoutMillis, Calls.memoryUsage(key, options));
    }

    public CompletableFuture<Map<String, Long>> memoryStats() {
        return memoryStats(commandTimeoutMillis);
    }

    public CompletableFuture<Map<String, Long>> memoryStats(long commandTimeoutMillis) {
        return complete(commandTimeoutMillis, Calls.memoryStats());
    }

    public CompletableFuture<String> objectEncoding(String key) {
        return objectEncoding(commandTimeoutMillis, key);
    }

    public CompletableFuture<String> objectEncoding(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.objectEncoding(key));
    }

    public CompletableFuture<List<String>> keys(String pattern) {
        return keys(commandTimeoutMillis, pattern);
    }

    public CompletableFuture<List<String>> keys(long commandTimeoutMillis, String pattern) {
        return complete(commandTimeoutMillis, Calls.keys(pattern));
    }

    public CompletableFuture<KeyScan> scan(String cursor) {
        return scan(commandTimeoutMillis, cursor);
    }

    public CompletableFuture<KeyScan> scan(long commandTimeoutMillis, String cursor) {
        return complete(commandTimeoutMillis, Calls.scan(cursor));
    }

    public CompletableFuture<KeyScan> scan(String cursor, ScanOptions options) {
        return scan(commandTimeoutMillis, cursor, options);
    }

    public CompletableFuture<KeyScan> scan(long commandTimeoutMillis, String cursor, ScanOptions options) {
        return complete(commandTimeoutMillis, Calls.scan(cursor, options));
    }

    public CompletableFuture<Long> del(String... keys) {
        return del(commandTimeoutMillis, keys);
    }

    public CompletableFuture<Long> del(long commandTimeoutMillis, String... keys) {
        return complete(commandTimeoutMillis, Calls.del(keys));
    }

    public CompletableFuture<Long> exists(String... keys) {
        return exists(commandTimeoutMillis, keys);
    }

    public CompletableFuture<Long> exists(long commandTimeoutMillis, String... keys) {
        return complete(commandTimeoutMillis, Calls.exists(keys));
    }

    public CompletableFuture<Long> expire(String key, long seconds) {
        return expire(commandTimeoutMillis, key, seconds);
    }

    public CompletableFuture<Long> expire(long commandTimeoutMillis, String key, long seconds) {
        return complete(commandTimeoutMillis, Calls.expire(key, seconds));
    }

    public CompletableFuture<Long> expire(String key, long seconds, ExpireOptions options) {
        return expire(commandTimeoutMillis, key, seconds, options);
    }

    public CompletableFuture<Long> expire(long commandTimeoutMillis, String key, long seconds, ExpireOptions options) {
        return complete(commandTimeoutMillis, Calls.expire(key, seconds, options));
    }

    public CompletableFuture<Long> pexpire(String key, long milliseconds) {
        return pexpire(commandTimeoutMillis, key, milliseconds);
    }

    public CompletableFuture<Long> pexpire(long commandTimeoutMillis, String key, long milliseconds) {
        return complete(commandTimeoutMillis, Calls.pexpire(key, milliseconds));
    }

    public CompletableFuture<Long> pexpire(String key, long milliseconds, ExpireOptions options) {
        return pexpire(commandTimeoutMillis, key, milliseconds, options);
    }

    public CompletableFuture<Long> pexpire(long commandTimeoutMillis, String key, long milliseconds, ExpireOptions options) {
        return complete(commandTimeoutMillis, Calls.pexpire(key, milliseconds, options));
    }

    public CompletableFuture<Long> expireat(String key, long unixSeconds) {
        return expireat(commandTimeoutMillis, key, unixSeconds);
    }

    public CompletableFuture<Long> expireat(long commandTimeoutMillis, String key, long unixSeconds) {
        return complete(commandTimeoutMillis, Calls.expireat(key, unixSeconds));
    }

    public CompletableFuture<Long> expireat(String key, long unixSeconds, ExpireOptions options) {
        return expireat(commandTimeoutMillis, key, unixSeconds, options);
    }

    public CompletableFuture<Long> expireat(long commandTimeoutMillis, String key, long unixSeconds, ExpireOptions options) {
        return complete(commandTimeoutMillis, Calls.expireat(key, unixSeconds, options));
    }

    public CompletableFuture<Long> pexpireat(String key, long unixMilliseconds) {
        return pexpireat(commandTimeoutMillis, key, unixMilliseconds);
    }

    public CompletableFuture<Long> pexpireat(long commandTimeoutMillis, String key, long unixMilliseconds) {
        return complete(commandTimeoutMillis, Calls.pexpireat(key, unixMilliseconds));
    }

    public CompletableFuture<Long> pexpireat(String key, long unixMilliseconds, ExpireOptions options) {
        return pexpireat(commandTimeoutMillis, key, unixMilliseconds, options);
    }

    public CompletableFuture<Long> pexpireat(long commandTimeoutMillis, String key, long unixMilliseconds, ExpireOptions options) {
        return complete(commandTimeoutMillis, Calls.pexpireat(key, unixMilliseconds, options));
    }

    public CompletableFuture<Long> persist(String key) {
        return persist(commandTimeoutMillis, key);
    }

    public CompletableFuture<Long> persist(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.persist(key));
    }

    public CompletableFuture<Long> ttl(String key) {
        return ttl(commandTimeoutMillis, key);
    }

    public CompletableFuture<Long> ttl(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.ttl(key));
    }

    public CompletableFuture<Long> pttl(String key) {
        return pttl(commandTimeoutMillis, key);
    }

    public CompletableFuture<Long> pttl(long commandTimeoutMillis, String key) {
        return complete(commandTimeoutMillis, Calls.pttl(key));
    }

    public CompletableFuture<Long> pfadd(String key, String... elements) {
        return pfadd(commandTimeoutMillis, key, elements);
    }

    public CompletableFuture<Long> pfadd(long commandTimeoutMillis, String key, String... elements) {
        return complete(commandTimeoutMillis, Calls.pfadd(key, elements));
    }

    public CompletableFuture<Long> pfcount(String... keys) {
        return pfcount(commandTimeoutMillis, keys);
    }

    public CompletableFuture<Long> pfcount(long commandTimeoutMillis, String... keys) {
        return complete(commandTimeoutMillis, Calls.pfcount(keys));
    }

    public CompletableFuture<String> pfmerge(String destination, String... sources) {
        return pfmerge(commandTimeoutMillis, destination, sources);
    }

    public CompletableFuture<String> pfmerge(long commandTimeoutMillis, String destination, String... sources) {
        return complete(commandTimeoutMillis, Calls.pfmerge(destination, sources));
    }

    boolean isClosed() {
        return closed;
    }

    boolean inNormalMode() {
        return mode == Mode.NORMAL;
    }

    long commandTimeoutMillis() {
        return commandTimeoutMillis;
    }

    Transaction currentTransaction() {
        return transaction;
    }

    boolean hasUnpairedCommands() {
        return pendingCount != 0;
    }

    void finishTransaction(Transaction source) {
        source.markFinished();
        if (transaction != source) {
            return;
        }
        transaction = null;
        rawTransaction = false;
        if (!closed) {
            mode = Mode.NORMAL;
        }
    }

    CompletableFuture<Object> submitTransaction(long commandTimeoutMillis, Call<?> call) {
        validateArgs(call.args);
        if (isCommand(call.args[0], "MULTI")) {
            throw new IllegalStateException("connection is not in normal mode");
        }
        PendingKind kind = PendingKind.QUEUED;
        if (isCommand(call.args[0], "EXEC")) {
            kind = PendingKind.EXEC;
        } else if (isCommand(call.args[0], "DISCARD")) {
            kind = PendingKind.DISCARD;
        }
        return submit(commandTimeoutMillis, call, kind, null, null);
    }

    CompletableFuture<List<Object>> submitTypedExec() {
        CompletableFuture<List<Object>> typed = new CompletableFuture<>();
        submit(commandTimeoutMillis, Call.raw("EXEC"), PendingKind.EXEC, null, typed);
        return typed;
    }

    private <T> CompletableFuture<T> complete(long commandTimeoutMillis, Call<T> call) {
        if (mode != Mode.NORMAL) {
            throw new IllegalStateException("connection is not in normal mode");
        }
        CompletableFuture<Object> raw = command(commandTimeoutMillis, call.args);
        CompletableFuture<T> typed = new CompletableFuture<>();
        raw.whenComplete((value, error) -> {
            if (error != null) {
                typed.completeExceptionally(unwrap(error));
                return;
            }
            try {
                typed.complete(call.decode(value));
            } catch (RuntimeException failure) {
                typed.completeExceptionally(failure);
            }
        });
        return typed;
    }

    private void noteExecResults(List<Call<?>> queued, RespClientCodec.RespReply reply) {
        if (queued.isEmpty() || reply.kind() != RespClientCodec.RespReply.Kind.ARRAY || reply.values() == null) {
            return;
        }
        List<RespClientCodec.RespReply> results = reply.values();
        int count = Math.min(queued.size(), results.size());
        for (int i = 0; i < count; i++) {
            RespClientCodec.RespReply result = results.get(i);
            if (result.kind() != RespClientCodec.RespReply.Kind.SIMPLE_STRING) {
                continue;
            }
            // 在转换整个 EXEC 结果前记录实际成功的 SELECT / QUIT；其他条目的 UTF-8 失败不能漏掉已执行的 DB 变化。
            byte[] text = result.bytes();
            if (text == null || text.length != 2 || text[0] != 'O' || text[1] != 'K') {
                continue;
            }
            noteSuccessfulCommand(queued.get(i).args);
        }
    }

    private CompletableFuture<Object> submit(
            long commandTimeoutMillis,
            Call<?> call,
            PendingKind kind,
            Runnable prelude,
            CompletableFuture<List<Object>> typedExec
    ) {
        // 参数先在调用方线程编完。UTF-8 失败时事件循环上还没有这条命令。
        byte[] payload = RespClientCodec.encodeCommand(encodeArgs(call.args));
        CompletableFuture<Object> future = new CompletableFuture<>();
        Pending created = new Pending(future, call, kind, commandTimeoutMillis, typedExec);
        Runnable accept = () -> acceptOnEventLoop(created, payload, prelude);
        if (channel.eventLoop().inEventLoop()) {
            accept.run();
            return future;
        }
        io.netty.util.concurrent.Future<?> accepted = channel.eventLoop().submit(accept);
        try {
            accepted.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectionException("interrupted while writing a command", e);
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = unwrap(e.getCause() == null ? e : e.getCause());
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new ConnectionException("failed to write a command", cause);
        }
        return future;
    }

    private void acceptOnEventLoop(Pending created, byte[] payload, Runnable prelude) {
        if (closed) {
            throw new IllegalStateException("connection is closed");
        }
        boolean enteredTransaction = false;
        try {
            if (prelude != null) {
                prelude.run();
                enteredTransaction = created.kind == PendingKind.MULTI;
            }
            if (!channel.isWritable()) {
                throw new IllegalStateException("connection is not writable");
            }
            channel.acceptWrite(payload);
            pending.addLast(created);
            pendingCount = pending.size();
            created.timer = channel.eventLoop().schedule(
                    () -> expire(created),
                    created.timeoutMillis,
                    TimeUnit.MILLISECONDS
            );
        } catch (RuntimeException failure) {
            if (enteredTransaction && pending.stream().noneMatch(item -> item.kind == PendingKind.MULTI)) {
                rollbackTransaction();
            }
            throw failure;
        }
    }

    private void beginRawTransaction() {
        if (mode != Mode.NORMAL || !pending.isEmpty()) {
            throw new IllegalStateException(mode != Mode.NORMAL
                    ? "connection is not in normal mode"
                    : "connection has unpaired commands");
        }
        mode = Mode.TRANSACTION;
        transaction = new Transaction(this);
        rawTransaction = true;
    }

    private void rollbackTransaction() {
        if (transaction != null) {
            transaction.markFinished();
        }
        transaction = null;
        rawTransaction = false;
        transactionBatch.clear();
        if (!closed) {
            mode = Mode.NORMAL;
        }
    }

    private void expire(Pending created) {
        if (created.userCompleted || closed) {
            return;
        }
        // 超时只失败这一条。命令留在队列里，晚到的回复仍由它消化，避免配给后面的命令。
        // exec() 交给调用方的是 typedExec，不是这条原始 Future。
        created.timedOut = true;
        CommandTimeoutException failure = new CommandTimeoutException("timed out waiting for a reply", null);
        completeUser(created, null, failure);
        failTypedExec(created, failure);
    }

    private void onReply(RespClientCodec.RespReply reply) {
        Pending created = pending.pollFirst();
        pendingCount = pending.size();
        if (created == null) {
            onTransportFailure(new IOException("unexpected RESP reply"));
            return;
        }
        // 事务命令的 Future 要到 EXEC 才完成。QUEUED 只表示已经入队，超时继续算到那时。
        if (created.kind != PendingKind.QUEUED) {
            cancelTimer(created);
        }
        try {
            switch (created.kind) {
                case NORMAL -> onNormal(created, reply);
                case MULTI -> onMulti(created, reply);
                case QUEUED -> onQueued(created, reply);
                case EXEC -> onExec(created, reply);
                case DISCARD -> onDiscard(created, reply);
            }
        } catch (RuntimeException failure) {
            completeUser(created, null, failure);
        }
    }

    private void onNormal(Pending created, RespClientCodec.RespReply reply) {
        if (reply.kind() == RespClientCodec.RespReply.Kind.ERROR) {
            if (!created.timedOut) {
                completeUser(created, null, serverError(reply));
            }
            return;
        }
        try {
            Object value = convert(reply, false);
            applySideEffects(created.call.args, value);
            if (!created.timedOut) {
                completeUser(created, value, null);
            }
        } catch (IOException failure) {
            // 整帧已经读完，只是客户端不接受这一种 RESP。后面的回复仍能对齐。
            if (!created.timedOut) {
                completeUser(created, null, new ConnectionException(failure.getMessage(), failure));
            }
        }
    }

    private void onMulti(Pending created, RespClientCodec.RespReply reply) {
        if (reply.kind() == RespClientCodec.RespReply.Kind.ERROR) {
            completeUser(created, null, serverError(reply));
            onTransportFailure(new IOException("MULTI was rejected"));
            return;
        }
        try {
            Object value = convert(reply, false);
            if (!"OK".equals(value)) {
                String actual = value == null ? "null" : value.getClass().getSimpleName();
                completeUser(created, null, new DecodeException("reply was " + actual + ", expected OK", null));
                onTransportFailure(new IOException("MULTI was rejected"));
                return;
            }
            if (!created.timedOut) {
                completeUser(created, value, null);
            }
        } catch (IOException failure) {
            completeUser(created, null, new ConnectionException(failure.getMessage(), failure));
            onTransportFailure(failure);
        }
    }

    private void onQueued(Pending created, RespClientCodec.RespReply reply) {
        if (reply.kind() == RespClientCodec.RespReply.Kind.ERROR) {
            if (!created.timedOut) {
                completeUser(created, null, serverError(reply));
            }
            return;
        }
        try {
            Object value = convert(reply, false);
            if (!"QUEUED".equals(value)) {
                String actual = value == null ? "null" : value.getClass().getSimpleName();
                if (!created.timedOut) {
                    completeUser(created, null, new DecodeException("reply was " + actual + ", expected QUEUED", null));
                }
                return;
            }
            transactionBatch.add(created);
        } catch (IOException failure) {
            if (!created.timedOut) {
                completeUser(created, null, new ConnectionException(failure.getMessage(), failure));
            }
        }
    }

    private void onExec(Pending created, RespClientCodec.RespReply reply) {
        if (reply.kind() == RespClientCodec.RespReply.Kind.ERROR) {
            ServerException failure = serverError(reply);
            failBatch(failure);
            if (!created.timedOut) {
                completeUser(created, null, failure);
            }
            failTypedExec(created, failure);
            finishCurrentTransaction();
            return;
        }
        try {
            noteExecResults(batchCalls(), reply);
            List<Object> results = finishExec(created, reply);
            if (!created.timedOut) {
                completeUser(created, results, null);
            }
            finishCurrentTransaction();
        } catch (IOException failure) {
            failTypedExec(created, failure);
            onTransportFailure(failure);
        } catch (RuntimeException failure) {
            failBatch(failure);
            if (!created.timedOut) {
                completeUser(created, null, failure);
            }
            failTypedExec(created, failure);
            finishCurrentTransaction();
        }
    }

    private void onDiscard(Pending created, RespClientCodec.RespReply reply) {
        if (reply.kind() == RespClientCodec.RespReply.Kind.ERROR) {
            if (!created.timedOut) {
                completeUser(created, null, serverError(reply));
            }
            return;
        }
        try {
            Object value = convert(reply, false);
            if (!"OK".equals(value)) {
                String actual = value == null ? "null" : value.getClass().getSimpleName();
                if (!created.timedOut) {
                    completeUser(created, null, new DecodeException("reply was " + actual + ", expected OK", null));
                }
                return;
            }
            failBatch(new IllegalStateException("transaction discarded"));
            finishCurrentTransaction();
            if (!created.timedOut) {
                completeUser(created, value, null);
            }
        } catch (IOException failure) {
            if (!created.timedOut) {
                completeUser(created, null, new ConnectionException(failure.getMessage(), failure));
            }
        }
    }

    private void onTransportFailure(Throwable cause) {
        if (closed) {
            return;
        }
        closed = true;
        Throwable failure = cause instanceof ConnectionException || cause instanceof CommandTimeoutException
                ? cause
                : new ConnectionException(cause.getMessage() == null ? "connection closed" : cause.getMessage(), cause);
        failPending(failure);
        finishCurrentTransaction();
        channel.closeChannel();
        channel.shutdownOwnedGroup();
    }

    private void failPending(Throwable cause) {
        Pending created;
        while ((created = pending.pollFirst()) != null) {
            cancelTimer(created);
            completeUser(created, null, cause);
            failTypedExec(created, cause);
        }
        pendingCount = 0;
        failBatch(cause);
    }

    private void failBatch(Throwable cause) {
        for (Pending queued : transactionBatch) {
            completeUser(queued, null, cause);
        }
        transactionBatch.clear();
    }

    private List<Call<?>> batchCalls() {
        List<Call<?>> calls = new ArrayList<>(transactionBatch.size());
        for (Pending queued : transactionBatch) {
            calls.add(queued.call);
        }
        return calls;
    }

    private void finishCurrentTransaction() {
        if (transaction != null) {
            finishTransaction(transaction);
        } else if (!closed) {
            mode = Mode.NORMAL;
            rawTransaction = false;
        }
    }

    private void completeUser(Pending created, Object value, Throwable error) {
        if (created.userCompleted) {
            return;
        }
        created.userCompleted = true;
        cancelTimer(created);
        if (error != null) {
            created.future.completeExceptionally(error);
        } else {
            created.future.complete(value);
        }
    }

    private void cancelTimer(Pending created) {
        if (created.timer != null) {
            created.timer.cancel(false);
            created.timer = null;
        }
    }

    private void applySideEffects(String[] args, Object value) {
        if (isCommand(args[0], "QUIT")) {
            // 调用方先拿到 OK，随后这条连接关掉。服务端错误不会走到这里。
            close();
            return;
        }
        if (isCommand(args[0], "SELECT") && args.length >= 2 && value instanceof String) {
            Integer index = parseDbIndex(args[1]);
            if (index != null) {
                database = index;
            }
        }
    }

    private static ServerException serverError(RespClientCodec.RespReply reply) {
        return new ServerException(replyText(reply));
    }

    private static Object await(CompletableFuture<Object> future) {
        try {
            return future.join();
        } catch (CompletionException failure) {
            Throwable cause = unwrap(failure);
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw failure;
        }
    }

    static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static final class Pending {
        private final CompletableFuture<Object> future;
        private final Call<?> call;
        private final PendingKind kind;
        private final long timeoutMillis;
        private final CompletableFuture<List<Object>> typedExec;
        private ScheduledFuture<?> timer;
        private boolean timedOut;
        private boolean userCompleted;

        private Pending(
                CompletableFuture<Object> future,
                Call<?> call,
                PendingKind kind,
                long timeoutMillis,
                CompletableFuture<List<Object>> typedExec
        ) {
            this.future = future;
            this.call = call;
            this.kind = kind;
            this.timeoutMillis = timeoutMillis;
            this.typedExec = typedExec;
        }
    }

    static Object convert(RespClientCodec.RespReply reply, boolean preserveErrors) throws IOException {
        // 先检查整帧的协议种类；较早元素的 UTF-8 失败不能掩盖后面必须关连接的 RESP3 标记。
        requireResp2(reply);
        return convertValue(reply, preserveErrors);
    }

    private static void requireResp2(RespClientCodec.RespReply reply) throws IOException {
        switch (reply.kind()) {
            case MAP, SET, NULL_TYPE -> throw new IOException("RESP3 reply marker: " + reply.kind());
            case ARRAY -> {
                if (reply.values() != null) {
                    for (RespClientCodec.RespReply element : reply.values()) {
                        requireResp2(element);
                    }
                }
            }
            default -> { }
        }
    }

    private static Object convertValue(RespClientCodec.RespReply reply, boolean preserveErrors) throws IOException {
        return switch (reply.kind()) {
            case SIMPLE_STRING -> replyText(reply);
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
            case ERROR -> {
                if (preserveErrors) {
                    yield new ServerException(replyText(reply));
                }
                // 收成文本只发生在 preserveErrors == false。类型化 exec() 传入 true，单条错误仍是 ServerException。
                yield replyText(reply);
            }
            case ARRAY -> convertAggregate(reply.values(), preserveErrors);
            // 任何深度的 %、~、_ 都和顶层标记一样变成 IOException，不收成列表或 null。
            case MAP, SET, NULL_TYPE -> throw new IOException("RESP3 reply marker: " + reply.kind());
        };
    }

    private static List<Object> convertAggregate(List<RespClientCodec.RespReply> values, boolean preserveErrors)
            throws IOException {
        if (values == null) {
            throw new DecodeException("aggregate reply has no elements", null);
        }
        List<Object> converted = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            converted.add(convertValue(values.get(i), preserveErrors));
        }
        return converted;
    }

    private static String replyText(RespClientCodec.RespReply reply) {
        byte[] bytes = reply.bytes();
        if (bytes != null) {
            return decodeReplyText(bytes);
        }
        String text = reply.text();
        if (text == null) {
            throw new DecodeException("text reply has no value", null);
        }
        return text;
    }

    void noteSuccessfulCommand(String[] args) {
        applySideEffects(args, "OK");
    }

    private List<Object> finishExec(Pending exec, RespClientCodec.RespReply reply) throws IOException {
        requireResp2(reply);
        if (reply.kind() != RespClientCodec.RespReply.Kind.ARRAY || reply.values() == null) {
            throw new DecodeException("reply was " + reply.kind() + ", expected array", null);
        }
        List<RespClientCodec.RespReply> elements = reply.values();
        if (elements.size() != transactionBatch.size()) {
            throw new DecodeException(
                    "reply was array of " + elements.size() + ", expected " + transactionBatch.size(), null);
        }
        // 原始 EXEC 保持回复形状，单条错误是原文。类型化 exec() 用每条命令自己的解码，单条错误是 ServerException。
        List<Object> rawResults = new ArrayList<>(elements.size());
        List<Object> typedResults = new ArrayList<>(elements.size());
        for (int i = 0; i < elements.size(); i++) {
            Pending queued = transactionBatch.get(i);
            RespClientCodec.RespReply element = elements.get(i);
            if (element.kind() == RespClientCodec.RespReply.Kind.ERROR) {
                ServerException failure = serverError(element);
                rawResults.add(failure.getMessage());
                typedResults.add(failure);
                if (!queued.timedOut) {
                    completeUser(queued, null, failure);
                }
                continue;
            }
            try {
                Object rawValue = convert(element, false);
                rawResults.add(rawValue);
                if (rawTransaction) {
                    typedResults.add(rawValue);
                    if (!queued.timedOut) {
                        completeUser(queued, rawValue, null);
                    }
                    continue;
                }
                try {
                    Object typedValue = queued.call.decode(convert(element, true));
                    typedResults.add(typedValue);
                    if (!queued.timedOut) {
                        completeUser(queued, typedValue, null);
                    }
                } catch (DecodeException failure) {
                    typedResults.add(failure);
                    if (!queued.timedOut) {
                        completeUser(queued, null, failure);
                    }
                }
            } catch (DecodeException failure) {
                rawResults.add(failure);
                typedResults.add(failure);
                if (!queued.timedOut) {
                    completeUser(queued, null, failure);
                }
            }
        }
        transactionBatch.clear();
        if (exec.typedExec != null && !exec.timedOut) {
            exec.typedExec.complete(typedResults);
        }
        return rawResults;
    }

    private static void failTypedExec(Pending created, Throwable cause) {
        if (created.typedExec != null && !created.typedExec.isDone()) {
            created.typedExec.completeExceptionally(cause);
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

    static boolean isCommand(String name, String expected) {
        return expected.equalsIgnoreCase(name);
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

}
