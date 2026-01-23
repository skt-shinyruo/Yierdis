package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;

import java.util.Arrays;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;
import static yier.bubu.redis.testutil.TestDbs.runUnsafeOffHeap;

public class HashCommandTest {
    @Test
    public void hsetHgetHlenAndHgetallAreBinarySafe() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = new byte[]{'h', 0, (byte) 0xFF};
            byte[] f1 = new byte[]{0, 'f', 1};
            byte[] v1 = new byte[]{(byte) 0xFF, 0, 'v'};
            byte[] f2 = new byte[]{'k'};
            byte[] v2 = new byte[]{'\n'};

            RespInteger added = (RespInteger) client.execute(Arrays.asList(
                    b("HSET"),
                    key,
                    f1, v1,
                    f2, v2
            ));
            Assert.assertEquals(2, added.value());

            RespInteger added2 = (RespInteger) client.execute(Arrays.asList(
                    b("HSET"),
                    key,
                    f1, v2
            ));
            Assert.assertEquals(0, added2.value());

            RespInteger hlen = (RespInteger) client.execute(Arrays.asList(b("HLEN"), key));
            Assert.assertEquals(2, hlen.value());

            RespObject hget = client.execute(Arrays.asList(b("HGET"), key, f1));
            Assert.assertTrue(hget instanceof RespBulkString);
            Assert.assertArrayEquals(v2, ((RespBulkString) hget).data());

            RespArray all = (RespArray) client.execute(Arrays.asList(b("HGETALL"), key));
            Assert.assertEquals(4, all.values().size());
            assertContainsPair(all, f1, v2);
            assertContainsPair(all, f2, v2);
            }
        });
    }

    @Test
    public void hdelRemovesHashKeyWhenEmpty() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = new byte[]{0, 'h'};
            byte[] field = new byte[]{(byte) 0xFF};
            byte[] value = new byte[]{0, 1, 2};

            Assert.assertTrue(client.execute(Arrays.asList(b("HSET"), key, field, value)) instanceof RespInteger);

            RespInteger removedMissing = (RespInteger) client.execute(Arrays.asList(b("HDEL"), key, new byte[]{'x'}));
            Assert.assertEquals(0, removedMissing.value());

            RespInteger removed = (RespInteger) client.execute(Arrays.asList(b("HDEL"), key, field));
            Assert.assertEquals(1, removed.value());

            RespInteger exists = (RespInteger) client.execute(Arrays.asList(b("EXISTS"), key));
            Assert.assertEquals(0, exists.value());

            RespSimpleString type = (RespSimpleString) client.execute(Arrays.asList(b("TYPE"), key));
            Assert.assertEquals("none", type.value());

            RespInteger hlen = (RespInteger) client.execute(Arrays.asList(b("HLEN"), key));
            Assert.assertEquals(0, hlen.value());

            RespArray all = (RespArray) client.execute(Arrays.asList(b("HGETALL"), key));
            Assert.assertTrue(all.values().isEmpty());
            }
        });
    }

    @Test
    public void hashUpgradesAfterManyFieldsAndKeepsData() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = b("big-hash");

            int fields = 513; // > HashValue.LISTPACK_MAX_ENTRIES
            long addedTotal = 0;
            for (int i = 0; i < fields; i++) {
                RespInteger added = (RespInteger) client.execute(Arrays.asList(b("HSET"), key, b("f" + i), b("v" + i)));
                addedTotal += added.value();
            }
            Assert.assertEquals(fields, addedTotal);

            RespInteger hlen = (RespInteger) client.execute(Arrays.asList(b("HLEN"), key));
            Assert.assertEquals(fields, hlen.value());

            RespBulkString v0 = (RespBulkString) client.execute(Arrays.asList(b("HGET"), key, b("f0")));
            Assert.assertEquals("v0", v0.asString());

            RespBulkString vLast = (RespBulkString) client.execute(Arrays.asList(b("HGET"), key, b("f" + (fields - 1))));
            Assert.assertEquals("v" + (fields - 1), vLast.asString());

            RespInteger updated = (RespInteger) client.execute(Arrays.asList(b("HSET"), key, b("f0"), b("v0-new")));
            Assert.assertEquals(0L, updated.value());
            RespBulkString v0New = (RespBulkString) client.execute(Arrays.asList(b("HGET"), key, b("f0")));
            Assert.assertEquals("v0-new", v0New.asString());
            }
        });
    }

    @Test
    public void offHeapHashStartsAsListpackAndUpgradesToHashtableAfterThreshold() {
        runUnsafeOffHeap(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                // 512 is YierdisEncodingThresholds.HASH_MAX_LISTPACK_ENTRIES (kept package-private).
                int threshold = 512;

                Assert.assertTrue(client.execute(cmd("HSET", "h", "f0", "v0")) instanceof RespInteger);
                Assert.assertEquals("listpack", ((RespSimpleString) client.execute(cmd("OBJECT", "ENCODING", "h"))).value());

                for (int i = 1; i < threshold; i++) {
                    Assert.assertTrue(client.execute(cmd("HSET", "h", "f" + i, "v" + i)) instanceof RespInteger);
                }
                Assert.assertEquals("listpack", ((RespSimpleString) client.execute(cmd("OBJECT", "ENCODING", "h"))).value());

                // Updating an existing field at the threshold should NOT trigger an upgrade.
                Assert.assertTrue(client.execute(cmd("HSET", "h", "f0", "v0-new")) instanceof RespInteger);
                Assert.assertEquals("listpack", ((RespSimpleString) client.execute(cmd("OBJECT", "ENCODING", "h"))).value());

                // Adding a new field beyond the threshold should upgrade to hashtable.
                Assert.assertTrue(client.execute(cmd("HSET", "h", "fx", "vx")) instanceof RespInteger);
                Assert.assertEquals("hashtable", ((RespSimpleString) client.execute(cmd("OBJECT", "ENCODING", "h"))).value());
            }
        });
    }

    private static void assertContainsPair(RespArray arr, byte[] field, byte[] value) {
        List<RespObject> values = arr.values();
        for (int i = 0; i + 1 < values.size(); i += 2) {
            RespObject k = values.get(i);
            RespObject v = values.get(i + 1);
            if (!(k instanceof RespBulkString) || !(v instanceof RespBulkString)) {
                continue;
            }
            if (Arrays.equals(field, ((RespBulkString) k).data()) && Arrays.equals(value, ((RespBulkString) v).data())) {
                return;
            }
        }
        Assert.fail("Missing pair in HGETALL response");
    }
}
