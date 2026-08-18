package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyNullArray;
import yier.bubu.redis.testutil.ReplyObject;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import static yier.bubu.redis.testutil.ReplyAssertions.assertArraySize;
import static yier.bubu.redis.testutil.ReplyAssertions.assertBulkString;
import static yier.bubu.redis.testutil.ReplyAssertions.assertInteger;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class DefaultCommandRegistrationTest {
    private static final Set<String> DEFAULT_COMMANDS = Set.of(
            "APPEND", "AUTH", "BITCOUNT", "CLIENT", "COMMAND", "DECR", "DEL", "DISCARD",
            "ECHO", "EXEC", "EXISTS", "EXPIRE", "EXPIREAT", "FLUSHDB", "GET", "GETBIT",
            "HDEL", "HGET", "HGETALL", "HLEN", "HSCAN", "HSET", "INCR", "KEYS", "LPOP",
            "LPUSH", "LRANGE", "MEMORY", "MULTI", "OBJECT", "PERSIST", "PEXPIRE", "PEXPIREAT",
            "PFADD", "PFCOUNT", "PFMERGE", "PING", "PTTL", "QUIT", "RPOP", "RPUSH", "SADD",
            "SCAN", "SCARD", "SELECT", "SET", "SETBIT", "SISMEMBER", "SMEMBERS", "SREM",
            "SSCAN", "STRLEN", "TTL", "TYPE", "ZADD", "ZRANGE", "ZRANGEBYSCORE", "ZREM",
            "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZREVRANGE", "ZREVRANGEBYSCORE", "ZSCAN"
    );

    public static Set<String> defaultCommandNames() {
        return DEFAULT_COMMANDS;
    }

    @Test
    public void defaultCompositionRegistersEveryCommand() {
        forEachDb(db -> {
            FastTestClient client = new FastTestClient(TestCommandComposition.createDispatcher(db));
            ReplyArray commands = assertArraySize(DEFAULT_COMMANDS.size(), client.execute(cmd("COMMAND")));
            Set<String> names = new HashSet<>();
            for (ReplyObject entry : commands.values()) {
                ReplyArray commandInfo = (ReplyArray) entry;
                names.add(((ReplyBulkString) commandInfo.values().get(0)).asString().toUpperCase(Locale.ROOT));
            }
            Assert.assertEquals(DEFAULT_COMMANDS, names);

            assertInteger(DEFAULT_COMMANDS.size(), client.execute(cmd("COMMAND", "COUNT")));
            ReplyArray info = assertArraySize(
                    2,
                    client.execute(cmd("COMMAND", "INFO", "PING", "missing"))
            );
            assertBulkString("ping", assertArraySize(6, info.values().get(0)).values().get(0));
            Assert.assertTrue(info.values().get(1) instanceof ReplyNullArray);
        });
    }
}
