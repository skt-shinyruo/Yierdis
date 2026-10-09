package yier.bubu.redis.integration.command;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyObject;

import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class ScanOptionsCommandTest {
    private static final String SYNTAX_ERROR = "ERR syntax error";

    @Test
    public void scanTypeKeepsOnlyKeysOfThatType() {
        forEachDb(db -> {
            FastTestClient client = clientWithEveryType(db);
            Map<String, Set<String>> expected = Map.of(
                    "string", Set.of("s", "hll"),
                    "list", Set.of("l"),
                    "set", Set.of("st"),
                    "zset", Set.of("z"),
                    "hash", Set.of("h"),
                    "STRING", Set.of("s", "hll"),
                    "HaSh", Set.of("h")
            );
            for (Map.Entry<String, Set<String>> entry : expected.entrySet()) {
                Assert.assertEquals(
                        "SCAN TYPE " + entry.getKey(),
                        entry.getValue(),
                        scanAll(client, "TYPE", entry.getKey())
                );
            }
            Assert.assertEquals(Set.of("s"), scanAll(client, "MATCH", "s*", "TYPE", "string"));
        });
    }

    @Test
    public void scanTypeUnknownToYierdisMatchesNoKeyButStillIterates() {
        forEachDb(db -> {
            FastTestClient client = clientWithEveryType(db);
            for (int index = 0; index < 200; index++) {
                client.execute(cmd("SET", "pad:" + index, "v"));
            }
            // Redis 8 仍把未知 TYPE 当作“不匹配任何 key”而不是报错（db.c 里的报错分支被注释掉）；
            // stream 等 Yierdis 不存储的类型同样只会得到空页，但 cursor 照常推进到 0。
            for (String type : List.of("bogus", "none", "stream", "array", "")) {
                String cursor = "0";
                int pages = 0;
                do {
                    ScanPage page = scan(client, "SCAN", cursor, "COUNT", "5", "TYPE", type);
                    Assert.assertTrue("SCAN TYPE " + type + " returned keys", page.keys().isEmpty());
                    cursor = page.cursor();
                    Assert.assertTrue("SCAN TYPE " + type + " did not terminate", ++pages < 4096);
                } while (!"0".equals(cursor));
                Assert.assertTrue("SCAN TYPE " + type + " must take more than one page", pages > 1);
            }
        });
    }

    @Test
    public void lastScanTypeWins() {
        forEachDb(db -> {
            FastTestClient client = clientWithEveryType(db);
            Assert.assertEquals(Set.of("l"), scanAll(client, "TYPE", "bogus", "TYPE", "list"));
            Assert.assertEquals(Set.of(), scanAll(client, "TYPE", "list", "TYPE", "bogus"));
        });
    }

    @Test
    public void scanTypeInsideMultiReplaysTheSameFilterAtExec() {
        forEachDb(db -> {
            FastTestClient client = clientWithEveryType(db);
            client.execute(cmd("MULTI"));
            client.execute(cmd("SCAN", "0", "COUNT", "1000", "TYPE", "list"));
            client.execute(cmd("SCAN", "0", "COUNT", "1000", "TYPE", "bogus"));
            ReplyObject exec = client.execute(cmd("EXEC"));
            Assert.assertTrue("expected EXEC array, got " + exec, exec instanceof ReplyArray);
            List<ReplyObject> replies = ((ReplyArray) exec).values();
            Assert.assertEquals(List.of("l"), page(replies.get(0)).keys());
            Assert.assertEquals(List.of(), page(replies.get(1)).keys());
        });
    }

    @Test
    public void typeOptionIsSyntaxErrorWhereRedisDoesNotAcceptIt() {
        forEachDb(db -> {
            FastTestClient client = clientWithEveryType(db);
            assertError(client.execute(cmd("SCAN", "0", "TYPE")), SYNTAX_ERROR);
            assertError(client.execute(cmd("HSCAN", "h", "0", "TYPE", "hash")), SYNTAX_ERROR);
            assertError(client.execute(cmd("SSCAN", "st", "0", "TYPE", "set")), SYNTAX_ERROR);
            assertError(client.execute(cmd("ZSCAN", "z", "0", "TYPE", "zset")), SYNTAX_ERROR);
        });
    }

    private static FastTestClient clientWithEveryType(YierdisDb db) {
        CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
        FastTestClient client = new FastTestClient(dispatcher);
        client.execute(cmd("SET", "s", "v"));
        client.execute(cmd("PFADD", "hll", "a"));
        client.execute(cmd("RPUSH", "l", "a"));
        client.execute(cmd("SADD", "st", "a", "b"));
        client.execute(cmd("ZADD", "z", "1", "a"));
        client.execute(cmd("HSET", "h", "f", "v"));
        return client;
    }

    private static Set<String> scanAll(FastTestClient client, String... options) {
        Set<String> seen = new HashSet<>();
        String cursor = "0";
        int pages = 0;
        do {
            String[] args = new String[4 + options.length];
            args[0] = "SCAN";
            args[1] = cursor;
            args[2] = "COUNT";
            args[3] = "3";
            System.arraycopy(options, 0, args, 4, options.length);
            ScanPage page = scan(client, args);
            seen.addAll(page.keys());
            cursor = page.cursor();
            Assert.assertTrue("SCAN did not terminate", ++pages < 4096);
        } while (!"0".equals(cursor));
        return seen;
    }

    private static ScanPage scan(FastTestClient client, String... args) {
        return page(client.execute(cmd(args)));
    }

    private static ScanPage page(ReplyObject reply) {
        Assert.assertTrue("expected scan array, got " + reply, reply instanceof ReplyArray);
        List<ReplyObject> outer = ((ReplyArray) reply).values();
        Assert.assertEquals(2, outer.size());
        List<String> keys = new ArrayList<>();
        for (ReplyObject key : ((ReplyArray) outer.get(1)).values()) {
            keys.add(((ReplyBulkString) key).asString());
        }
        return new ScanPage(((ReplyBulkString) outer.get(0)).asString(), keys);
    }

    private static void assertError(ReplyObject reply, String message) {
        Assert.assertTrue("expected error " + message + ", got " + reply, reply instanceof ReplyError);
        Assert.assertEquals(message, ((ReplyError) reply).message());
    }

    private record ScanPage(String cursor, List<String> keys) {
    }
}
