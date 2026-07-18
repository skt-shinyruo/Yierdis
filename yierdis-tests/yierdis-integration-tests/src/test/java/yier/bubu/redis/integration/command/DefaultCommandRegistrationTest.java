package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyNullArray;
import yier.bubu.redis.testutil.ReplyObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static yier.bubu.redis.testutil.ReplyAssertions.assertArraySize;
import static yier.bubu.redis.testutil.ReplyAssertions.assertBulkString;
import static yier.bubu.redis.testutil.ReplyAssertions.assertInteger;
import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class DefaultCommandRegistrationTest {
    private static final Map<String, CommandMetadata> DEFAULT_COMMAND_METADATA = defaultCommandMetadata();
    private static final Set<String> DEFAULT_COMMANDS = DEFAULT_COMMAND_METADATA.keySet();

    public static Set<String> defaultCommandNames() {
        return DEFAULT_COMMANDS;
    }

    @Test
    public void commandRegistryListsEveryDefaultCommand() {
        withClient(client -> {
            ReplyArray commands = assertArraySize(DEFAULT_COMMANDS.size(), client.execute(cmd("COMMAND")));
            Set<String> actualNames = new LinkedHashSet<>();
            for (ReplyObject entry : commands.values()) {
                ReplyArray commandInfo = (ReplyArray) entry;
                actualNames.add(((ReplyBulkString) commandInfo.values().get(0)).asString().toUpperCase(Locale.ROOT));
            }
            Assert.assertEquals(DEFAULT_COMMANDS, actualNames);
        });
    }

    @Test
    public void commandRegistryCountsEveryDefaultCommand() {
        withClient(client ->
                assertInteger(DEFAULT_COMMANDS.size(), client.execute(cmd("COMMAND", "COUNT")))
        );
    }

    @Test
    public void testCommandCompositionListsEveryDefaultCommandIncludingTransactions() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandComposition.createProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                assertInteger(DEFAULT_COMMANDS.size(), client.execute(cmd("COMMAND", "COUNT")));
            }
        });
    }

    @Test
    public void commandRegistryReportsMetadataForEveryDefaultCommand() {
        withClient(client -> {
            ReplyArray info = assertArraySize(
                    DEFAULT_COMMAND_METADATA.size(),
                    client.execute(commandInfoRequestForEveryDefaultCommand())
            );
            int index = 0;
            for (Map.Entry<String, CommandMetadata> entry : DEFAULT_COMMAND_METADATA.entrySet()) {
                CommandMetadata metadata = entry.getValue();
                assertCommandInfo(
                        info.values().get(index++),
                        entry.getKey().toLowerCase(Locale.ROOT),
                        metadata.arity(),
                        metadata.firstKey(),
                        metadata.lastKey(),
                        metadata.step()
                );
            }
        });
    }

    @Test
    public void commandRegistryInfoReturnsNullArrayForUnknownCommand() {
        withClient(client -> {
            ReplyArray info = assertArraySize(1, client.execute(cmd("COMMAND", "INFO", "no-such-command")));
            Assert.assertTrue(info.values().get(0) instanceof ReplyNullArray);
        });
    }

    private static Map<String, CommandMetadata> defaultCommandMetadata() {
        LinkedHashMap<String, CommandMetadata> metadata = new LinkedHashMap<>();
        metadata(metadata, "AUTH", -2, 0, 0, 0);
        metadata(metadata, "APPEND", 3, 1, 1, 1);
        metadata(metadata, "BITCOUNT", -2, 1, 1, 1);
        metadata(metadata, "CLIENT", -2, 0, 0, 0);
        metadata(metadata, "COMMAND", -1, 0, 0, 0);
        metadata(metadata, "DECR", 2, 1, 1, 1);
        metadata(metadata, "DEL", -2, 1, -1, 1);
        metadata(metadata, "DISCARD", 1, 0, 0, 0);
        metadata(metadata, "ECHO", 2, 0, 0, 0);
        metadata(metadata, "EXEC", 1, 0, 0, 0);
        metadata(metadata, "EXISTS", -2, 1, -1, 1);
        metadata(metadata, "EXPIRE", 3, 1, 1, 1);
        metadata(metadata, "EXPIREAT", 3, 1, 1, 1);
        metadata(metadata, "FLUSHDB", -1, 0, 0, 0);
        metadata(metadata, "GET", 2, 1, 1, 1);
        metadata(metadata, "GETBIT", 3, 1, 1, 1);
        metadata(metadata, "HDEL", -3, 1, 1, 1);
        metadata(metadata, "HGET", 3, 1, 1, 1);
        metadata(metadata, "HGETALL", 2, 1, 1, 1);
        metadata(metadata, "HLEN", 2, 1, 1, 1);
        metadata(metadata, "HSET", -4, 1, 1, 1);
        metadata(metadata, "INCR", 2, 1, 1, 1);
        metadata(metadata, "KEYS", 2, 0, 0, 0);
        metadata(metadata, "LPOP", -2, 1, 1, 1);
        metadata(metadata, "LPUSH", -3, 1, 1, 1);
        metadata(metadata, "LRANGE", 4, 1, 1, 1);
        metadata(metadata, "MEMORY", -2, 0, 0, 0);
        metadata(metadata, "MULTI", 1, 0, 0, 0);
        metadata(metadata, "OBJECT", -2, 0, 0, 0);
        metadata(metadata, "PERSIST", 2, 1, 1, 1);
        metadata(metadata, "PEXPIRE", 3, 1, 1, 1);
        metadata(metadata, "PEXPIREAT", 3, 1, 1, 1);
        metadata(metadata, "PFADD", -3, 1, 1, 1);
        metadata(metadata, "PFCOUNT", -2, 1, -1, 1);
        metadata(metadata, "PFMERGE", -3, 1, -1, 1);
        metadata(metadata, "PING", -1, 0, 0, 0);
        metadata(metadata, "PTTL", 2, 1, 1, 1);
        metadata(metadata, "QUIT", 1, 0, 0, 0);
        metadata(metadata, "RPOP", -2, 1, 1, 1);
        metadata(metadata, "RPUSH", -3, 1, 1, 1);
        metadata(metadata, "SADD", -3, 1, 1, 1);
        metadata(metadata, "SCAN", -2, 0, 0, 0);
        metadata(metadata, "SCARD", 2, 1, 1, 1);
        metadata(metadata, "SELECT", 2, 0, 0, 0);
        metadata(metadata, "SET", -3, 1, 1, 1);
        metadata(metadata, "SETBIT", 4, 1, 1, 1);
        metadata(metadata, "SISMEMBER", 3, 1, 1, 1);
        metadata(metadata, "SMEMBERS", 2, 1, 1, 1);
        metadata(metadata, "SREM", -3, 1, 1, 1);
        metadata(metadata, "STRLEN", 2, 1, 1, 1);
        metadata(metadata, "TTL", 2, 1, 1, 1);
        metadata(metadata, "TYPE", 2, 1, 1, 1);
        metadata(metadata, "ZADD", -4, 1, 1, 1);
        metadata(metadata, "ZRANGE", -4, 1, 1, 1);
        metadata(metadata, "ZRANGEBYSCORE", -4, 1, 1, 1);
        metadata(metadata, "ZREM", -3, 1, 1, 1);
        metadata(metadata, "ZREMRANGEBYRANK", 4, 1, 1, 1);
        metadata(metadata, "ZREMRANGEBYSCORE", 4, 1, 1, 1);
        metadata(metadata, "ZREVRANGE", -4, 1, 1, 1);
        metadata(metadata, "ZREVRANGEBYSCORE", -4, 1, 1, 1);
        return Collections.unmodifiableMap(metadata);
    }

    private static void metadata(
            Map<String, CommandMetadata> metadata,
            String name,
            long arity,
            long firstKey,
            long lastKey,
            long step
    ) {
        metadata.put(name, new CommandMetadata(arity, firstKey, lastKey, step));
    }

    private static List<byte[]> commandInfoRequestForEveryDefaultCommand() {
        List<byte[]> args = new ArrayList<>(DEFAULT_COMMAND_METADATA.size() + 2);
        args.add(b("COMMAND"));
        args.add(b("INFO"));
        for (String command : DEFAULT_COMMAND_METADATA.keySet()) {
            args.add(b(command));
        }
        return args;
    }

    private static void withClient(ClientCase test) {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandComposition.createProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                test.run(client);
            }
        });
    }

    private static void assertCommandInfo(ReplyObject reply, String name, long arity, long firstKey, long lastKey, long step) {
        ReplyArray info = (ReplyArray) reply;
        Assert.assertEquals(6, info.values().size());
        assertBulkString(name, info.values().get(0));
        Assert.assertEquals(arity, ((ReplyInteger) info.values().get(1)).value());
        Assert.assertTrue(info.values().get(2) instanceof ReplyArray);
        Assert.assertEquals(firstKey, ((ReplyInteger) info.values().get(3)).value());
        Assert.assertEquals(lastKey, ((ReplyInteger) info.values().get(4)).value());
        Assert.assertEquals(step, ((ReplyInteger) info.values().get(5)).value());
    }

    private record CommandMetadata(long arity, long firstKey, long lastKey, long step) {
    }

    @FunctionalInterface
    private interface ClientCase {
        void run(FastTestClient client);
    }
}
