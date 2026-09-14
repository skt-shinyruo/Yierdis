package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyNullArray;
import yier.bubu.redis.testutil.ReplyObject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
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

    @Test
    public void commandMetadataExposesNonZeroArityAndWellFormedKeySpecs() {
        forEachDb(db -> {
            FastTestClient client = new FastTestClient(TestCommandComposition.createDispatcher(db));
            ReplyArray commands = assertArraySize(DEFAULT_COMMANDS.size(), client.execute(cmd("COMMAND")));
            Map<String, ReplyArray> byName = new HashMap<>();
            for (ReplyObject entry : commands.values()) {
                ReplyArray commandInfo = assertArraySize(6, entry);
                byName.put(
                        ((ReplyBulkString) commandInfo.values().get(0)).asString().toUpperCase(Locale.ROOT),
                        commandInfo
                );
            }

            for (Map.Entry<String, ReplyArray> command : byName.entrySet()) {
                String name = command.getKey();
                ReplyArray commandInfo = command.getValue();
                long arity = ((ReplyInteger) commandInfo.values().get(1)).value();
                Assert.assertNotEquals(name + " must publish a non-zero arity", 0L, arity);
                long firstKey = ((ReplyInteger) commandInfo.values().get(3)).value();
                long lastKey = ((ReplyInteger) commandInfo.values().get(4)).value();
                long keyStep = ((ReplyInteger) commandInfo.values().get(5)).value();
                if (firstKey == 0) {
                    Assert.assertEquals(name + " keyless command must publish lastKey 0", 0L, lastKey);
                    Assert.assertEquals(name + " keyless command must publish keyStep 0", 0L, keyStep);
                } else {
                    Assert.assertTrue(name + " firstKey must be >= 1", firstKey >= 1L);
                    Assert.assertTrue(name + " keyed command must publish a positive keyStep", keyStep >= 1L);
                    Assert.assertTrue(
                            name + " lastKey must be -1 or >= firstKey",
                            lastKey == -1L || lastKey >= firstKey
                    );
                }
            }

            assertMetadata(byName.get("GET"), 2, 1, 1, 1);
            assertMetadata(byName.get("SET"), -3, 1, 1, 1);
            assertMetadata(byName.get("EXPIRE"), -3, 1, 1, 1);
            assertMetadata(byName.get("DEL"), -2, 1, -1, 1);
            assertMetadata(byName.get("PFMERGE"), -3, 1, -1, 1);
            assertMetadata(byName.get("PING"), -1, 0, 0, 0);
            assertMetadata(byName.get("MULTI"), 1, 0, 0, 0);
        });
    }

    private static void assertMetadata(
            ReplyArray commandInfo,
            long arity,
            long firstKey,
            long lastKey,
            long keyStep
    ) {
        Assert.assertNotNull(commandInfo);
        String name = ((ReplyBulkString) commandInfo.values().get(0)).asString();
        Assert.assertEquals(name + " arity", arity, ((ReplyInteger) commandInfo.values().get(1)).value());
        Assert.assertEquals(name + " firstKey", firstKey, ((ReplyInteger) commandInfo.values().get(3)).value());
        Assert.assertEquals(name + " lastKey", lastKey, ((ReplyInteger) commandInfo.values().get(4)).value());
        Assert.assertEquals(name + " keyStep", keyStep, ((ReplyInteger) commandInfo.values().get(5)).value());
    }
}
