package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandDispatcher;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyNull;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.util.ArrayList;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

// 原始回复对照 Redis 8.9.241。
// PFADD key：缺 key 是整数 1，已有 key 是整数 0。
// CLIENT SETNAME 非法名字：ERR Client names cannot contain spaces, newlines or special characters.
// 空名字的 SETNAME 是 OK，随后 GETNAME 是 nil。
// OBJECT / MEMORY 没有子命令：ERR wrong number of arguments for 'object' command（memory 同理）。
// 未知子命令按原样大小写回显：ERR unknown subcommand 'Foo'. Try OBJECT HELP.（MEMORY 同理）。
// 已知子命令参数个数不对：ERR wrong number of arguments for 'object|encoding' command，子命令名总是小写。
// MEMORY USAGE 选项写错或 SAMPLES 为负：ERR syntax error。SAMPLES 不是整数：ERR value is not an integer or out of range。
public class RedisCompatibilityBoundaryTest {
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
            assertError(client.execute(cmd("OBJECT", "ENCODING")),
                    "ERR wrong number of arguments for 'object|encoding' command");
            assertError(client.execute(cmd("OBJECT", "Encoding", "k", "extra")),
                    "ERR wrong number of arguments for 'object|encoding' command");
            assertError(client.execute(cmd("OBJECT", "help", "x")),
                    "ERR wrong number of arguments for 'object|help' command");
            assertError(client.execute(cmd("OBJECT", "foo")), "ERR unknown subcommand 'foo'. Try OBJECT HELP.");
            assertError(client.execute(cmd("OBJECT", "Foo", "k")), "ERR unknown subcommand 'Foo'. Try OBJECT HELP.");
            assertError(client.execute(cmd("OBJECT", "foo" + "x".repeat(300))),
                    "ERR unknown subcommand 'foo" + "x".repeat(125) + "'. Try OBJECT HELP.");
            assertError(client.execute(cmd("OBJECT", "a\nb")), "ERR unknown subcommand 'a b'. Try OBJECT HELP.");

            client.execute(cmd("SET", "k", "v"));
            Assert.assertEquals("embstr", ((ReplyBulkString) client.execute(
                    cmd("OBJECT", "Encoding", "k"))).asString());
        });
    }

    @Test
    public void memorySubcommandErrorsNameTheSubcommand() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            FastTestClient client = new FastTestClient(dispatcher);

            assertError(client.execute(cmd("MEMORY")), "ERR wrong number of arguments for 'memory' command");
            assertError(client.execute(cmd("MEMORY", "foo")), "ERR unknown subcommand 'foo'. Try MEMORY HELP.");
            assertError(client.execute(cmd("MEMORY", "Foo", "x")), "ERR unknown subcommand 'Foo'. Try MEMORY HELP.");
            assertError(client.execute(cmd("MEMORY", "usage")),
                    "ERR wrong number of arguments for 'memory|usage' command");
            assertError(client.execute(cmd("MEMORY", "Stats", "x")),
                    "ERR wrong number of arguments for 'memory|stats' command");
            assertError(client.execute(cmd("MEMORY", "HELP", "x")),
                    "ERR wrong number of arguments for 'memory|help' command");
        });
    }

    // HELP 的格式照 Redis addReplyHelp：首行用法、每个子命令一行签名加缩进说明、末尾 HELP；
    // 只列 Yierdis 实现了的子命令，说明文字逐字取自 Redis 8.9.241。
    @Test
    public void objectAndMemoryHelpListImplementedSubcommandsInRedisFormat() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            FastTestClient client = new FastTestClient(dispatcher);

            Assert.assertEquals(List.of(
                    "OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "ENCODING <key>",
                    "    Return the kind of internal representation used in order to store the value",
                    "    associated with a <key>.",
                    "HELP",
                    "    Print this help."
            ), simpleStrings(client.execute(cmd("OBJECT", "help"))));
            Assert.assertEquals(List.of(
                    "MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "STATS",
                    "    Return information about the memory usage of the server.",
                    "USAGE <key> [SAMPLES <count>]",
                    "    Return memory in bytes used by <key> and its value. Nested values are",
                    "    sampled up to <count> times (default: 5, 0 means sample all).",
                    "HELP",
                    "    Print this help."
            ), simpleStrings(client.execute(cmd("MEMORY", "HELP"))));
        });
    }

    private static List<String> simpleStrings(ReplyObject reply) {
        List<String> lines = new ArrayList<>();
        for (ReplyObject line : ((ReplyArray) reply).values()) {
            lines.add(((ReplySimpleString) line).value());
        }
        return lines;
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
                    "ERR wrong number of arguments for 'memory|usage' command");
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
