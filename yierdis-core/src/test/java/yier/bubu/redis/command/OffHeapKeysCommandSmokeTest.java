package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.offheap.unsafe.YierdisUnsafeOffHeapAllocator;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;
import yier.bubu.redis.testutil.FastTestClient;

import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;

public class OffHeapKeysCommandSmokeTest {
    @Test
    public void keysScanDelAndHllWorkWhenKeysStoredOffHeap() {
        YierdisUnsafeOffHeapAllocator allocator = new YierdisUnsafeOffHeapAllocator(0);
        YierdisDb db = new YierdisDb(allocator, true, 0, "noeviction", 5, 5, 5);
        try {
            db.bindToCurrentThread();
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                byte[] v = b("v");
                byte[] a1 = b("a1");
                byte[] b1 = b("b1");
                client.execute(Arrays.asList(b("SET"), a1, v));
                client.execute(Arrays.asList(b("SET"), b1, v));

                RespArray keys = (RespArray) client.execute(Arrays.asList(b("KEYS"), b("[ab]*")));
                Assert.assertTrue(containsBytes(keys, a1));
                Assert.assertTrue(containsBytes(keys, b1));

                RespArray scan = (RespArray) client.execute(Arrays.asList(
                        b("SCAN"), b("0"),
                        b("MATCH"), b("a*"),
                        b("COUNT"), b("10")
                ));
                Assert.assertEquals(2, scan.values().size());
                RespArray scanKeys = (RespArray) scan.values().get(1);
                Assert.assertTrue(containsBytes(scanKeys, a1));
                Assert.assertFalse(containsBytes(scanKeys, b1));

                RespInteger del = (RespInteger) client.execute(Arrays.asList(b("DEL"), a1, b1));
                Assert.assertEquals(2, del.value());

                client.execute(cmd("PFADD", "h1", "foo", "bar"));
                client.execute(cmd("PFADD", "h2", "bar", "baz"));
                RespInteger union = (RespInteger) client.execute(cmd("PFCOUNT", "h1", "h2"));
                Assert.assertEquals(3, union.value());

                RespObject ok = client.execute(cmd("PFMERGE", "hu", "h1", "h2"));
                Assert.assertTrue(ok instanceof RespSimpleString);

                RespInteger merged = (RespInteger) client.execute(cmd("PFCOUNT", "hu"));
                Assert.assertEquals(3, merged.value());
            }
        } finally {
            db.shutdown();
        }
    }

    private static boolean containsBytes(RespArray array, byte[] expected) {
        for (RespObject o : array.values()) {
            if (o instanceof RespBulkString && Arrays.equals(expected, ((RespBulkString) o).data())) {
                return true;
            }
        }
        return false;
    }
}

