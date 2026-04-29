package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyMap;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;
import static yier.bubu.redis.testutil.TestDbs.runDefaultFfm;

public class HashCommandTest {
    @Test
    public void hashCommandsUseReadWriteBoundariesInsteadOfLegacyValueOps() throws IOException {
        String source = Files.readString(Path.of(
                "..", "..", "yierdis-command", "yierdis-command-defaults", "src", "main", "java", "yier", "bubu", "redis", "command", "HashCommands.java"
        ));

        Assert.assertFalse(source.contains("eviction().prepareWrite("));
        Assert.assertFalse(source.contains("values().hashes()."));
    }

    @Test
    public void hsetHgetHlenAndHgetallAreBinarySafe() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = new byte[]{'h', 0, (byte) 0xFF};
            byte[] f1 = new byte[]{0, 'f', 1};
            byte[] v1 = new byte[]{(byte) 0xFF, 0, 'v'};
            byte[] f2 = new byte[]{'k'};
            byte[] v2 = new byte[]{'\n'};

            ReplyInteger added = (ReplyInteger) client.execute(Arrays.asList(
                    b("HSET"),
                    key,
                    f1, v1,
                    f2, v2
            ));
            Assert.assertEquals(2, added.value());

            ReplyInteger added2 = (ReplyInteger) client.execute(Arrays.asList(
                    b("HSET"),
                    key,
                    f1, v2
            ));
            Assert.assertEquals(0, added2.value());

            ReplyInteger hlen = (ReplyInteger) client.execute(Arrays.asList(b("HLEN"), key));
            Assert.assertEquals(2, hlen.value());

            ReplyObject hget = client.execute(Arrays.asList(b("HGET"), key, f1));
            Assert.assertTrue(hget instanceof ReplyBulkString);
            Assert.assertArrayEquals(v2, ((ReplyBulkString) hget).data());

	            ReplyMap all = (ReplyMap) client.execute(Arrays.asList(b("HGETALL"), key));
	            Assert.assertEquals(2, all.entries().size());
	            assertContainsPair(all, f1, v2);
	            assertContainsPair(all, f2, v2);
            }
        });
    }

    @Test
    public void hdelRemovesHashKeyWhenEmpty() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = new byte[]{0, 'h'};
            byte[] field = new byte[]{(byte) 0xFF};
            byte[] value = new byte[]{0, 1, 2};

            Assert.assertTrue(client.execute(Arrays.asList(b("HSET"), key, field, value)) instanceof ReplyInteger);

            ReplyInteger removedMissing = (ReplyInteger) client.execute(Arrays.asList(b("HDEL"), key, new byte[]{'x'}));
            Assert.assertEquals(0, removedMissing.value());

            ReplyInteger removed = (ReplyInteger) client.execute(Arrays.asList(b("HDEL"), key, field));
            Assert.assertEquals(1, removed.value());

            ReplyInteger exists = (ReplyInteger) client.execute(Arrays.asList(b("EXISTS"), key));
            Assert.assertEquals(0, exists.value());

            ReplySimpleString type = (ReplySimpleString) client.execute(Arrays.asList(b("TYPE"), key));
            Assert.assertEquals("none", type.value());

            ReplyInteger hlen = (ReplyInteger) client.execute(Arrays.asList(b("HLEN"), key));
            Assert.assertEquals(0, hlen.value());

	            ReplyMap all = (ReplyMap) client.execute(Arrays.asList(b("HGETALL"), key));
	            Assert.assertTrue(all.entries().isEmpty());
            }
        });
    }

    @Test
    public void hashUpgradesAfterManyFieldsAndKeepsData() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {

            byte[] key = b("big-hash");

            int fields = 513; // > HashValue.LISTPACK_MAX_ENTRIES
            long addedTotal = 0;
            for (int i = 0; i < fields; i++) {
                ReplyInteger added = (ReplyInteger) client.execute(Arrays.asList(b("HSET"), key, b("f" + i), b("v" + i)));
                addedTotal += added.value();
            }
            Assert.assertEquals(fields, addedTotal);

            ReplyInteger hlen = (ReplyInteger) client.execute(Arrays.asList(b("HLEN"), key));
            Assert.assertEquals(fields, hlen.value());

            ReplyBulkString v0 = (ReplyBulkString) client.execute(Arrays.asList(b("HGET"), key, b("f0")));
            Assert.assertEquals("v0", v0.asString());

            ReplyBulkString vLast = (ReplyBulkString) client.execute(Arrays.asList(b("HGET"), key, b("f" + (fields - 1))));
            Assert.assertEquals("v" + (fields - 1), vLast.asString());

            ReplyInteger updated = (ReplyInteger) client.execute(Arrays.asList(b("HSET"), key, b("f0"), b("v0-new")));
            Assert.assertEquals(0L, updated.value());
            ReplyBulkString v0New = (ReplyBulkString) client.execute(Arrays.asList(b("HGET"), key, b("f0")));
            Assert.assertEquals("v0-new", v0New.asString());
            }
        });
    }

    @Test
    public void ffmHashStartsAsListpackAndUpgradesToHashtableAfterThreshold() {
        runDefaultFfm(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                // 512 is YierdisEncodingThresholds.HASH_MAX_LISTPACK_ENTRIES (kept package-private).
                int threshold = 512;

                Assert.assertTrue(client.execute(cmd("HSET", "h", "f0", "v0")) instanceof ReplyInteger);
                Assert.assertEquals("listpack", ((ReplyBulkString) client.execute(cmd("OBJECT", "ENCODING", "h"))).asString());

                for (int i = 1; i < threshold; i++) {
                    Assert.assertTrue(client.execute(cmd("HSET", "h", "f" + i, "v" + i)) instanceof ReplyInteger);
                }
                Assert.assertEquals("listpack", ((ReplyBulkString) client.execute(cmd("OBJECT", "ENCODING", "h"))).asString());

                // Updating an existing field at the threshold should NOT trigger an upgrade.
                Assert.assertTrue(client.execute(cmd("HSET", "h", "f0", "v0-new")) instanceof ReplyInteger);
                Assert.assertEquals("listpack", ((ReplyBulkString) client.execute(cmd("OBJECT", "ENCODING", "h"))).asString());

                // Adding a new field beyond the threshold should upgrade to hashtable.
                Assert.assertTrue(client.execute(cmd("HSET", "h", "fx", "vx")) instanceof ReplyInteger);
                Assert.assertEquals("hashtable", ((ReplyBulkString) client.execute(cmd("OBJECT", "ENCODING", "h"))).asString());
            }
        });
    }

	    private static void assertContainsPair(ReplyMap map, byte[] field, byte[] value) {
	        List<ReplyMap.Entry> entries = map.entries();
	        for (ReplyMap.Entry e : entries) {
	            ReplyObject k = e.key();
	            ReplyObject v = e.value();
	            if (!(k instanceof ReplyBulkString) || !(v instanceof ReplyBulkString)) {
	                continue;
	            }
	            if (Arrays.equals(field, ((ReplyBulkString) k).data()) && Arrays.equals(value, ((ReplyBulkString) v).data())) {
	                return;
	            }
	        }
	        Assert.fail("Missing pair in HGETALL response");
	    }
}
