package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.createFfmDb;

public class OffHeapKeysCommandSmokeTest {
    @Test
    public void keysScanDelAndHllWorkWhenKeysStoredOffHeap() {
        YierdisDb db = createFfmDb(new DbEngineConfig(
                0,
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                5L,
                new DbDefragConfig(false, 0L, 0L, 0L)
        ), 0);
        try {
            db.bindToCurrentThread();
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                byte[] v = b("v");
                byte[] a1 = b("a1");
                byte[] b1 = b("b1");
                client.execute(Arrays.asList(b("SET"), a1, v));
                client.execute(Arrays.asList(b("SET"), b1, v));

                ReplyArray keys = (ReplyArray) client.execute(Arrays.asList(b("KEYS"), b("[ab]*")));
                Assert.assertTrue(containsBytes(keys, a1));
                Assert.assertTrue(containsBytes(keys, b1));

                ReplyArray scan = (ReplyArray) client.execute(Arrays.asList(
                        b("SCAN"), b("0"),
                        b("MATCH"), b("a*"),
                        b("COUNT"), b("10")
                ));
                Assert.assertEquals(2, scan.values().size());
                ReplyArray scanKeys = (ReplyArray) scan.values().get(1);
                Assert.assertTrue(containsBytes(scanKeys, a1));
                Assert.assertFalse(containsBytes(scanKeys, b1));

                ReplyInteger del = (ReplyInteger) client.execute(Arrays.asList(b("DEL"), a1, b1));
                Assert.assertEquals(2, del.value());

                client.execute(cmd("PFADD", "h1", "foo", "bar"));
                client.execute(cmd("PFADD", "h2", "bar", "baz"));
                ReplyInteger union = (ReplyInteger) client.execute(cmd("PFCOUNT", "h1", "h2"));
                Assert.assertEquals(3, union.value());

                ReplyObject ok = client.execute(cmd("PFMERGE", "hu", "h1", "h2"));
                Assert.assertTrue(ok instanceof ReplySimpleString);

                ReplyInteger merged = (ReplyInteger) client.execute(cmd("PFCOUNT", "hu"));
                Assert.assertEquals(3, merged.value());
            }
        } finally {
            db.shutdown();
        }
    }

    private static boolean containsBytes(ReplyArray array, byte[] expected) {
        for (ReplyObject o : array.values()) {
            if (o instanceof ReplyBulkString && Arrays.equals(expected, ((ReplyBulkString) o).data())) {
                return true;
            }
        }
        return false;
    }
}
