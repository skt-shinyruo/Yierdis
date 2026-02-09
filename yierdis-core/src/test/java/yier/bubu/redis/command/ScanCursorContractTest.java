package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class ScanCursorContractTest {
    @Test
    public void cursorTerminatesAtZeroAndMakesProgress() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
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
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
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
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
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

    private static long parseCursor(ReplyArray reply) {
        Assert.assertNotNull(reply);
        Assert.assertNotNull(reply.values());
        Assert.assertEquals(2, reply.values().size());
        ReplyBulkString cursorOut = (ReplyBulkString) reply.values().get(0);
        Assert.assertNotNull(cursorOut.data());
        return Long.parseLong(new String(cursorOut.data(), StandardCharsets.US_ASCII));
    }
}
