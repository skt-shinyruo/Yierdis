package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplyNull;
import yier.bubu.redis.testutil.ReplyNullArray;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class Milestone1CompatTest {
    @Test
    public void scanMatchAndCountEventuallyReturnsAllMatchingKeys() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("k1"), b("v"))) instanceof ReplySimpleString);
                Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("k2"), b("v"))) instanceof ReplySimpleString);
                Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("k3"), b("v"))) instanceof ReplySimpleString);
                Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("x1"), b("v"))) instanceof ReplySimpleString);

                long cursor = 0L;
                Set<String> seen = new HashSet<>();
                for (int round = 0; round < 50; round++) {
                    ReplyArray reply = (ReplyArray) client.execute(Arrays.asList(
                            b("SCAN"),
                            Long.toString(cursor).getBytes(StandardCharsets.US_ASCII),
                            b("MATCH"), b("k*"),
                            b("COUNT"), b("2")
                    ));
                    Assert.assertNotNull(reply.values());
                    Assert.assertEquals(2, reply.values().size());

                    ReplyBulkString cursorOut = (ReplyBulkString) reply.values().get(0);
                    ReplyArray keys = (ReplyArray) reply.values().get(1);

                    cursor = Long.parseLong(new String(cursorOut.data(), StandardCharsets.US_ASCII));
                    if (keys.values() != null) {
                        for (ReplyObject o : keys.values()) {
                            seen.add(((ReplyBulkString) o).asString());
                        }
                    }
                    if (cursor == 0L) {
                        break;
                    }
                }

                Assert.assertTrue(seen.contains("k1"));
                Assert.assertTrue(seen.contains("k2"));
                Assert.assertTrue(seen.contains("k3"));
                Assert.assertFalse(seen.contains("x1"));
            }
        });
    }

    @Test
    public void ttlFamilyPersistAndPexpireMatchRedisLikeConventions() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("k"), b("v"))) instanceof ReplySimpleString);

                ReplyInteger pttlNoTtl = (ReplyInteger) client.execute(Arrays.asList(b("PTTL"), b("k")));
                Assert.assertEquals(-1L, pttlNoTtl.value());

                ReplyInteger persistNoTtl = (ReplyInteger) client.execute(Arrays.asList(b("PERSIST"), b("k")));
                Assert.assertEquals(0L, persistNoTtl.value());

                ReplyInteger pexpireZeroDeletes = (ReplyInteger) client.execute(Arrays.asList(b("PEXPIRE"), b("k"), b("0")));
                Assert.assertEquals(1L, pexpireZeroDeletes.value());

                ReplyInteger ttlMissing = (ReplyInteger) client.execute(Arrays.asList(b("TTL"), b("k")));
                Assert.assertEquals(-2L, ttlMissing.value());
            }
        });
    }

    @Test
    public void setGetAndKeepTtlBehaveAsExpected() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("k"), b("v"), b("EX"), b("5"))) instanceof ReplySimpleString);
                Assert.assertTrue(((ReplyInteger) client.execute(Arrays.asList(b("TTL"), b("k")))).value() > 0L);

                ReplyBulkString old = (ReplyBulkString) client.execute(Arrays.asList(b("SET"), b("k"), b("v2"), b("KEEPTTL"), b("GET")));
                Assert.assertEquals("v", old.asString());
                Assert.assertTrue(((ReplyInteger) client.execute(Arrays.asList(b("TTL"), b("k")))).value() > 0L);

	                ReplyObject nxGet = client.execute(Arrays.asList(b("SET"), b("k"), b("v3"), b("NX"), b("GET")));
	                Assert.assertTrue(nxGet instanceof ReplyNull);
            }
        });
    }

    @Test
    public void setRejectsConflictingModeOptions() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                ReplyError err = (ReplyError) client.execute(Arrays.asList(b("SET"), b("k"), b("v"), b("NX"), b("XX")));
                Assert.assertEquals("ERR syntax error", err.message());
            }
        });
    }

    @Test
    public void flushdbValidatesOptions() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("k"), b("v"))) instanceof ReplySimpleString);
                Assert.assertTrue(client.execute(Arrays.asList(b("FLUSHDB"), b("ASYNC"))) instanceof ReplySimpleString);
	                Assert.assertTrue(client.execute(Arrays.asList(b("GET"), b("k"))) instanceof ReplyNull);

                ReplyError err = (ReplyError) client.execute(Arrays.asList(b("FLUSHDB"), b("X")));
                Assert.assertEquals("ERR syntax error", err.message());
            }
        });
    }

    @Test
    public void lpopCountHandlesNullArrayAndEmptyArray() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
		            try (FastTestClient client = new FastTestClient(processor)) {
		                Assert.assertTrue(client.execute(Arrays.asList(b("LPOP"), b("nope"))) instanceof ReplyNull);

		                Assert.assertTrue(client.execute(Arrays.asList(b("LPOP"), b("nope"), b("2"))) instanceof ReplyNullArray);

		                ReplyArray zeroCount = (ReplyArray) client.execute(Arrays.asList(b("LPOP"), b("nope"), b("0")));
		                Assert.assertEquals(List.of(), zeroCount.values());

                ReplyError negative = (ReplyError) client.execute(Arrays.asList(b("LPOP"), b("nope"), b("-1")));
                Assert.assertEquals("ERR value is not an integer or out of range", negative.message());
            }
        });
    }
}
