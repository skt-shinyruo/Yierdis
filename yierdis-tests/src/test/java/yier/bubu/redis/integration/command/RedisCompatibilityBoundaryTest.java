package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyNull;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

// 原始回复对照 Redis 8.9.241。
// PFADD key：缺 key 是整数 1，已有 key 是整数 0。
// CLIENT SETNAME 非法名字：ERR Client names cannot contain spaces, newlines or special characters.
// 空名字的 SETNAME 是 OK，随后 GETNAME 是 nil。
// OBJECT 没有子命令：ERR wrong number of arguments for 'object' command。
// OBJECT 未知子命令或参数个数不对：ERR Unknown subcommand or wrong number of arguments for 'OBJECT'. Try OBJECT HELP.
// MEMORY USAGE 缺 key：ERR wrong number of arguments for 'memory' command。
// MEMORY USAGE 选项写错或 SAMPLES 为负：ERR syntax error。SAMPLES 不是整数：ERR value is not an integer or out of range。
public class RedisCompatibilityBoundaryTest {
    private static final String OBJECT_SUBCOMMAND_ERROR =
            "ERR Unknown subcommand or wrong number of arguments for 'OBJECT'. Try OBJECT HELP.";
    private static final String CLIENT_NAME_ERROR =
            "ERR Client names cannot contain spaces, newlines or special characters.";

    @Test
    public void pfaddWithOnlyAKeyCreatesOrLeavesTheHyperLogLog() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            FastTestClient client = new FastTestClient(dispatcher);

            ReplyInteger created = (ReplyInteger) client.execute(cmd("PFADD", "h"));
            Assert.assertEquals(1, created.value());
            ReplyInteger existing = (ReplyInteger) client.execute(cmd("PFADD", "h"));
            Assert.assertEquals(0, existing.value());
            ReplyInteger counted = (ReplyInteger) client.execute(cmd("PFCOUNT", "h"));
            Assert.assertEquals(0, counted.value());
        });
    }

    @Test
    public void clientSetnameKeepsPrintableAsciiAndClearsOnEmpty() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            FastTestClient client = new FastTestClient(dispatcher);

            Assert.assertEquals("OK", ((ReplySimpleString) client.execute(
                    cmd("CLIENT", "SETNAME", "ok_name"))).value());
            Assert.assertEquals("ok_name", ((ReplyBulkString) client.execute(
                    cmd("CLIENT", "GETNAME"))).asString());

            Assert.assertEquals("OK", ((ReplySimpleString) client.execute(
                    cmd("CLIENT", "SETNAME", ""))).value());
            Assert.assertTrue(client.execute(cmd("CLIENT", "GETNAME")) instanceof ReplyNull);

            assertError(client.execute(cmd("CLIENT", "SETNAME", "bad name")), CLIENT_NAME_ERROR);
            assertError(client.execute(cmd("CLIENT", "SETNAME", "bad\nname")), CLIENT_NAME_ERROR);
            assertError(client.execute(cmd("CLIENT", "SETNAME", "café")), CLIENT_NAME_ERROR);
            Assert.assertTrue(client.execute(cmd("CLIENT", "GETNAME")) instanceof ReplyNull);
        });
    }

    @Test
    public void objectErrorsSeparateMissingSubcommandFromSubcommandSyntax() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            FastTestClient client = new FastTestClient(dispatcher);

            assertError(
                    client.execute(cmd("OBJECT")),
                    "ERR wrong number of arguments for 'object' command");
            assertError(client.execute(cmd("OBJECT", "ENCODING")), OBJECT_SUBCOMMAND_ERROR);
            assertError(client.execute(cmd("OBJECT", "ENCODING", "k", "extra")), OBJECT_SUBCOMMAND_ERROR);
            assertError(client.execute(cmd("OBJECT", "REFCOUNT", "k")), OBJECT_SUBCOMMAND_ERROR);
            assertError(client.execute(cmd("OBJECT", "HELP")), OBJECT_SUBCOMMAND_ERROR);

            client.execute(cmd("SET", "k", "v"));
            Assert.assertEquals("embstr", ((ReplyBulkString) client.execute(
                    cmd("OBJECT", "ENCODING", "k"))).asString());
        });
    }

    @Test
    public void memoryUsageAcceptsSamplesWithoutChangingTheByteCount() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            FastTestClient client = new FastTestClient(dispatcher);
            client.execute(cmd("SET", "k", "v"));

            long usage = ((ReplyInteger) client.execute(cmd("MEMORY", "USAGE", "k"))).value();
            Assert.assertEquals(usage, ((ReplyInteger) client.execute(
                    cmd("MEMORY", "USAGE", "k", "SAMPLES", "0"))).value());
            Assert.assertEquals(usage, ((ReplyInteger) client.execute(
                    cmd("MEMORY", "USAGE", "k", "SAMPLES", "5"))).value());

            assertError(
                    client.execute(cmd("MEMORY", "USAGE")),
                    "ERR wrong number of arguments for 'memory' command");
            assertError(client.execute(cmd("MEMORY", "USAGE", "k", "SAMPLES", "-1")), "ERR syntax error");
            assertError(client.execute(cmd("MEMORY", "USAGE", "k", "SAMPLES")), "ERR syntax error");
            assertError(client.execute(cmd("MEMORY", "USAGE", "k", "COUNT", "1")), "ERR syntax error");
            assertError(
                    client.execute(cmd("MEMORY", "USAGE", "k", "SAMPLES", "nope")),
                    "ERR value is not an integer or out of range");
        });
    }

    private static void assertError(ReplyObject reply, String message) {
        Assert.assertTrue(reply instanceof ReplyError);
        Assert.assertEquals(message, ((ReplyError) reply).message());
    }
}
