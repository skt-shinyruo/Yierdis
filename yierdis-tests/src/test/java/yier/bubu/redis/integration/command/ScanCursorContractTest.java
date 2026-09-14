package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.kernel.CommandDispatcher;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyObject;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class ScanCursorContractTest {
    @Test
    public void cursorTerminatesAtZeroAndMakesProgress() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                for (int i = 0; i < 50; i++) {
                    client.execute(Arrays.asList(b("SET"), b("k" + i), b("v")));
                }

                long cursor = 0L;
                for (int round = 0; round < 200; round++) {
                    ReplyArray reply = (ReplyArray) client.execute(Arrays.asList(
                            b("SCAN"),
                            Long.toString(cursor).getBytes(StandardCharsets.US_ASCII),
                            b("COUNT"), b("3")
                    ));
                    long next = parseCursor(reply);
                    if (next == 0L) {
                        cursor = 0L;
                        break;
                    }
                    Assert.assertTrue("expected cursor progress, got next=" + next + " from cursor=" + cursor, next > cursor);
                    cursor = next;
                }

                Assert.assertEquals("expected scan to terminate", 0L, cursor);
            }
        });
    }

    @Test
    public void countAndMatchNeverDeadlockEvenWhenNoKeyMatches() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                for (int i = 0; i < 20; i++) {
                    client.execute(Arrays.asList(b("SET"), b("k" + i), b("v")));
                }

                long cursor = 0L;
                for (int round = 0; round < 20; round++) {
                    ReplyArray reply = (ReplyArray) client.execute(Arrays.asList(
                            b("SCAN"),
                            Long.toString(cursor).getBytes(StandardCharsets.US_ASCII),
                            b("MATCH"), b("nomatch*"),
                            b("COUNT"), b("1")
                    ));
                    cursor = parseCursor(reply);
                    if (cursor == 0L) {
                        break;
                    }
                }
                Assert.assertEquals("expected scan to terminate even with no matches", 0L, cursor);
            }
        });
    }

    @Test
    public void cursorTerminatesEvenWhenDatasetMutatesDuringRehash() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                // Fill enough keys to trigger growth/rehash at least once.
                for (int i = 0; i < 200; i++) {
                    client.execute(Arrays.asList(b("SET"), b("k" + i), b("v")));
                }

                long cursor = 0L;
                for (int round = 0; round < 500; round++) {
                    ReplyArray reply = (ReplyArray) client.execute(Arrays.asList(
                            b("SCAN"),
                            Long.toString(cursor).getBytes(StandardCharsets.US_ASCII),
                            b("COUNT"), b("5")
                    ));
                    long next = parseCursor(reply);

                    // Mutate the dataset between scan calls:
                    // - insert (may start/advance rehash)
                    // - delete
                    // - expire immediately (deletes)
                    if (round % 3 == 0) {
                        client.execute(Arrays.asList(b("SET"), b("x" + round), b("v")));
                    } else if (round % 3 == 1) {
                        client.execute(Arrays.asList(b("DEL"), b("k" + (round % 200))));
                    } else {
                        client.execute(Arrays.asList(b("EXPIRE"), b("k" + (round % 200)), b("0")));
                    }

                    if (next == 0L) {
                        cursor = 0L;
                        break;
                    }
                    Assert.assertNotEquals("expected cursor progress", cursor, next);
                    cursor = next;
                }

                Assert.assertEquals("expected scan to terminate", 0L, cursor);
            }
        });
    }

    @Test
    public void arbitraryNonNegativeCursorRestartsIterationAndTerminatesAtZero() {
        forEachDb(db -> {
            CommandDispatcher dispatcher = TestCommandComposition.createDispatcher(db);
            {
                FastTestClient client = new FastTestClient(dispatcher);
                for (int i = 0; i < 30; i++) {
                    client.execute(Arrays.asList(b("SET"), b("k" + i), b("v")));
                }

                // 不透明 cursor：phase 位超出内部 0/1 约定或 generation 不匹配的值都必须按
                // 重启迭代处理（允许重复），不得报错，结束仍回 0。
                List<String> cursors = new ArrayList<>(List.of(
                        "8589934592",
                        "12884901888",
                        "9223372036854775807"
                ));
                // 伪造一个 generation 匹配但 phase 位非法的 cursor：改写在线 cursor 的 phase 位。
                long live = parseCursor((ReplyArray) client.execute(Arrays.asList(
                        b("SCAN"), b("0"), b("COUNT"), b("1"))));
                Assert.assertNotEquals(0L, live);
                cursors.add(Long.toString(live | (2L << 32)));

                for (String initial : cursors) {
                    Set<String> seen = new HashSet<>();
                    String cursor = initial;
                    for (int round = 0; round < 200; round++) {
                        ReplyArray reply = (ReplyArray) client.execute(Arrays.asList(
                                b("SCAN"), b(cursor), b("COUNT"), b("4")));
                        for (ReplyObject element : ((ReplyArray) reply.values().get(1)).values()) {
                            seen.add(((ReplyBulkString) element).asString());
                        }
                        cursor = ((ReplyBulkString) reply.values().get(0)).asString();
                        if ("0".equals(cursor)) {
                            break;
                        }
                    }
                    Assert.assertEquals(
                            "expected scan from opaque cursor " + initial + " to terminate at 0",
                            "0",
                            cursor
                    );
                    Assert.assertEquals(
                            "expected restart from opaque cursor " + initial + " to cover every key",
                            30,
                            seen.size()
                    );
                }
            }
        });
    }

    private static long parseCursor(ReplyArray reply) {
        Assert.assertNotNull(reply);
        Assert.assertNotNull(reply.values());
        Assert.assertEquals(2, reply.values().size());
        ReplyBulkString cursorOut = (ReplyBulkString) reply.values().get(0);
        Assert.assertNotNull(cursorOut.data());
        return Long.parseLong(new String(cursorOut.data(), StandardCharsets.US_ASCII));
    }
}
