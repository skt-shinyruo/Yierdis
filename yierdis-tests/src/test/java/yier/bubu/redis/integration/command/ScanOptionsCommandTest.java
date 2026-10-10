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
import yier.bubu.redis.testutil.ReplySimpleString;

import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class ScanOptionsCommandTest {
    private static final String SYNTAX_ERROR = "ERR syntax error";
    private static final String INTEGER_ERROR = "ERR value is not an integer or out of range";
    private static final String INVALID_CURSOR = "ERR invalid cursor";
    private static final String NOVALUES_ERROR = "ERR NOVALUES option can only be used in HSCAN";
    private static final List<List<String>> SCAN_PREFIXES = List.of(
            List.of("SCAN"),
            List.of("HSCAN", "h"),
            List.of("SSCAN", "st"),
            List.of("ZSCAN", "z")
    );

    @Test
    public void countBelowOneIsSyntaxErrorAndNonIntegerCountIsIntegerError() {
        forEachDb(db -> {
            FastTestClient client = clientWithEveryType(db);
            for (List<String> prefix : SCAN_PREFIXES) {
                assertError(client.execute(scanCommand(prefix, "0", "COUNT", "0")), SYNTAX_ERROR);
                assertError(client.execute(scanCommand(prefix, "0", "COUNT", "-1")), SYNTAX_ERROR);
                assertError(client.execute(scanCommand(prefix, "0", "COUNT", "abc")), INTEGER_ERROR);
                assertError(client.execute(scanCommand(prefix, "0", "COUNT", "+5")), INTEGER_ERROR);
                assertError(
                        client.execute(scanCommand(prefix, "0", "COUNT", "9223372036854775808")),
                        INTEGER_ERROR
                );
                // Redis 的 COUNT 是 long：超过 int 的值照样是合法的工作量 hint。
                page(client.execute(scanCommand(prefix, "0", "COUNT", "2147483648")));
                page(client.execute(scanCommand(prefix, "0", "COUNT", "9223372036854775807")));

                // 和 Redis 一样逐个 option 解析，先失败的那个决定错误文案。
                assertError(client.execute(scanCommand(prefix, "0", "BOGUS", "COUNT", "abc")), SYNTAX_ERROR);
                assertError(client.execute(scanCommand(prefix, "0", "COUNT", "abc", "BOGUS")), INTEGER_ERROR);
                assertError(client.execute(scanCommand(prefix, "0", "COUNT")), SYNTAX_ERROR);
                assertError(client.execute(scanCommand(prefix, "0", "MATCH")), SYNTAX_ERROR);
            }
        });
    }

    @Test
    public void cursorsRedisCannotParseAreInvalidCursor() {
        forEachDb(db -> {
            FastTestClient client = clientWithEveryType(db);
            List<String> invalid = List.of(
                    "-1", "abc", "", " ", "1.5", "1 ", "0x10", "+", "-", "+-1", "--1", "1_0",
                    "18446744073709551616", "99999999999999999999",
                    // string2ll 能解析的负数一律拒绝，哪怕它在 strtoull 下会回绕成合法 u64。
                    "-9223372036854775808",
                    "\u00001"
            );
            for (List<String> prefix : SCAN_PREFIXES) {
                for (String cursor : invalid) {
                    assertError(
                            "cursor " + printable(cursor) + " on " + prefix.get(0),
                            client.execute(scanCommand(prefix, cursor)),
                            INVALID_CURSOR
                    );
                }
                // cursor 在 option 之前校验。
                assertError(client.execute(scanCommand(prefix, "abc", "COUNT", "0")), INVALID_CURSOR);
                assertError(client.execute(scanCommand(prefix, "-1", "NOVALUES")), INVALID_CURSOR);
            }
        });
    }

    @Test
    public void cursorsRedisAcceptsStillScanToCompletion() {
        forEachDb(db -> {
            FastTestClient client = clientWithEveryType(db);
            // Redis 先按 string2ll 解析，失败再退回 strtoull：前导空白、单个 +/-、前导零都能通过，
            // '-' 按 u64 取模回绕；参数里的 NUL 之后的字节不参与解析。
            List<String> accepted = List.of(
                    "18446744073709551615", "9223372036854775808", "9223372036854775807",
                    "00000000000000000000018446744073709551615",
                    " 1", "\t3", "\n3", "+1", "01", "-0", " -1",
                    "-9223372036854775809", "-18446744073709551615",
                    "3\u0000x"
            );
            for (List<String> prefix : SCAN_PREFIXES) {
                for (String initial : accepted) {
                    String cursor = initial;
                    int pages = 0;
                    do {
                        ReplyObject reply = client.execute(scanCommand(prefix, cursor, "COUNT", "3"));
                        Assert.assertTrue(
                                "cursor " + printable(initial) + " on " + prefix.get(0) + " got " + reply,
                                reply instanceof ReplyArray
                        );
                        cursor = page(reply).cursor();
                        Assert.assertTrue("scan did not terminate", ++pages < 4096);
                    } while (!"0".equals(cursor));
                }
            }
        });
    }

    @Test
    public void noValuesOutsideHscanUsesRedisMessage() {
        forEachDb(db -> {
            FastTestClient client = clientWithEveryType(db);
            assertError(client.execute(cmd("SCAN", "0", "NOVALUES")), NOVALUES_ERROR);
            assertError(client.execute(cmd("SSCAN", "st", "0", "NOVALUES")), NOVALUES_ERROR);
            assertError(client.execute(cmd("ZSCAN", "z", "0", "NOVALUES")), NOVALUES_ERROR);
            assertError(client.execute(cmd("SSCAN", "st", "0", "NOVALUES", "COUNT", "0")), NOVALUES_ERROR);
            assertError(client.execute(cmd("SSCAN", "st", "0", "COUNT", "0", "NOVALUES")), SYNTAX_ERROR);
            assertError(client.execute(cmd("HSCAN", "h", "0", "NOVALUES", "COUNT", "0")), SYNTAX_ERROR);
            Assert.assertEquals(List.of("f"), scan(client, "HSCAN", "h", "0", "NOVALUES").keys());
        });
    }

    @Test
    public void scanValidationErrorsInsideMultiFailOnlyThatCommandAtExec() {
        forEachDb(db -> {
            FastTestClient client = clientWithEveryType(db);
            client.execute(cmd("MULTI"));
            for (List<String> queued : List.of(
                    List.of("SCAN", "abc"),
                    List.of("SCAN", "0", "COUNT", "0"),
                    List.of("SSCAN", "st", "0", "NOVALUES"),
                    List.of("HSCAN", "h", "18446744073709551616"),
                    List.of("ZSCAN", "z", "0", "COUNT", "-1"),
                    List.of("SET", "kept", "1")
            )) {
                ReplyObject reply = client.execute(cmd(queued.toArray(String[]::new)));
                Assert.assertEquals("QUEUED", ((ReplySimpleString) reply).value());
            }
            List<ReplyObject> exec = ((ReplyArray) client.execute(cmd("EXEC"))).values();
            Assert.assertEquals(6, exec.size());
            assertError(exec.get(0), INVALID_CURSOR);
            assertError(exec.get(1), SYNTAX_ERROR);
            assertError(exec.get(2), NOVALUES_ERROR);
            assertError(exec.get(3), INVALID_CURSOR);
            assertError(exec.get(4), SYNTAX_ERROR);
            Assert.assertEquals("1", ((ReplyBulkString) client.execute(cmd("GET", "kept"))).asString());
        });
    }

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

    private static List<byte[]> scanCommand(List<String> prefix, String... rest) {
        String[] args = new String[prefix.size() + rest.length];
        for (int index = 0; index < prefix.size(); index++) {
            args[index] = prefix.get(index);
        }
        System.arraycopy(rest, 0, args, prefix.size(), rest.length);
        return cmd(args);
    }

    private static String printable(String value) {
        return value.replace("\u0000", "\\0").replace("\t", "\\t").replace("\n", "\\n");
    }

    private static void assertError(ReplyObject reply, String message) {
        assertError("", reply, message);
    }

    private static void assertError(String context, ReplyObject reply, String message) {
        Assert.assertTrue(context + ": expected error " + message + ", got " + reply, reply instanceof ReplyError);
        Assert.assertEquals(context, message, ((ReplyError) reply).message());
    }

    private record ScanPage(String cursor, List<String> keys) {
    }
}
