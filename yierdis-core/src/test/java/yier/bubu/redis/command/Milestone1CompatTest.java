package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;
import yier.bubu.redis.testutil.FastTestClient;

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
                Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("k1"), b("v"))) instanceof RespSimpleString);
                Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("k2"), b("v"))) instanceof RespSimpleString);
                Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("k3"), b("v"))) instanceof RespSimpleString);
                Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("x1"), b("v"))) instanceof RespSimpleString);

                long cursor = 0L;
                Set<String> seen = new HashSet<>();
                for (int round = 0; round < 50; round++) {
                    RespArray reply = (RespArray) client.execute(Arrays.asList(
                            b("SCAN"),
                            Long.toString(cursor).getBytes(StandardCharsets.US_ASCII),
                            b("MATCH"), b("k*"),
                            b("COUNT"), b("2")
                    ));
                    Assert.assertNotNull(reply.values());
                    Assert.assertEquals(2, reply.values().size());

                    RespBulkString cursorOut = (RespBulkString) reply.values().get(0);
                    RespArray keys = (RespArray) reply.values().get(1);

                    cursor = Long.parseLong(new String(cursorOut.data(), StandardCharsets.US_ASCII));
                    if (keys.values() != null) {
                        for (RespObject o : keys.values()) {
                            seen.add(((RespBulkString) o).asString());
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
                Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("k"), b("v"))) instanceof RespSimpleString);

                RespInteger pttlNoTtl = (RespInteger) client.execute(Arrays.asList(b("PTTL"), b("k")));
                Assert.assertEquals(-1L, pttlNoTtl.value());

                RespInteger persistNoTtl = (RespInteger) client.execute(Arrays.asList(b("PERSIST"), b("k")));
                Assert.assertEquals(0L, persistNoTtl.value());

                RespInteger pexpireZeroDeletes = (RespInteger) client.execute(Arrays.asList(b("PEXPIRE"), b("k"), b("0")));
                Assert.assertEquals(1L, pexpireZeroDeletes.value());

                RespInteger ttlMissing = (RespInteger) client.execute(Arrays.asList(b("TTL"), b("k")));
                Assert.assertEquals(-2L, ttlMissing.value());
            }
        });
    }

    @Test
    public void setGetAndKeepTtlBehaveAsExpected() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("k"), b("v"), b("EX"), b("5"))) instanceof RespSimpleString);
                Assert.assertTrue(((RespInteger) client.execute(Arrays.asList(b("TTL"), b("k")))).value() > 0L);

                RespBulkString old = (RespBulkString) client.execute(Arrays.asList(b("SET"), b("k"), b("v2"), b("KEEPTTL"), b("GET")));
                Assert.assertEquals("v", old.asString());
                Assert.assertTrue(((RespInteger) client.execute(Arrays.asList(b("TTL"), b("k")))).value() > 0L);

                RespBulkString nxGet = (RespBulkString) client.execute(Arrays.asList(b("SET"), b("k"), b("v3"), b("NX"), b("GET")));
                Assert.assertTrue(nxGet.isNull());
            }
        });
    }

    @Test
    public void setRejectsConflictingModeOptions() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                RespError err = (RespError) client.execute(Arrays.asList(b("SET"), b("k"), b("v"), b("NX"), b("XX")));
                Assert.assertEquals("ERR syntax error", err.message());
            }
        });
    }

    @Test
    public void flushdbValidatesOptions() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("k"), b("v"))) instanceof RespSimpleString);
                Assert.assertTrue(client.execute(Arrays.asList(b("FLUSHDB"), b("ASYNC"))) instanceof RespSimpleString);
                Assert.assertTrue(((RespBulkString) client.execute(Arrays.asList(b("GET"), b("k")))).isNull());

                RespError err = (RespError) client.execute(Arrays.asList(b("FLUSHDB"), b("X")));
                Assert.assertEquals("ERR syntax error", err.message());
            }
        });
    }

    @Test
    public void lpopCountHandlesNullArrayAndEmptyArray() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                RespBulkString missing = (RespBulkString) client.execute(Arrays.asList(b("LPOP"), b("nope")));
                Assert.assertTrue(missing.isNull());

                RespArray missingCount = (RespArray) client.execute(Arrays.asList(b("LPOP"), b("nope"), b("2")));
                Assert.assertTrue(missingCount.isNull());

                RespArray zeroCount = (RespArray) client.execute(Arrays.asList(b("LPOP"), b("nope"), b("0")));
                Assert.assertFalse(zeroCount.isNull());
                Assert.assertEquals(List.of(), zeroCount.values());

                RespError negative = (RespError) client.execute(Arrays.asList(b("LPOP"), b("nope"), b("-1")));
                Assert.assertEquals("ERR value is not an integer or out of range", negative.message());
            }
        });
    }
}

