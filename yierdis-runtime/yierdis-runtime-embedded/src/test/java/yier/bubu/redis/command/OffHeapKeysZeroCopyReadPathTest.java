package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplySimpleString;

import static yier.bubu.redis.testutil.TestBytes.cmd;

public class OffHeapKeysZeroCopyReadPathTest {
    @Test
    public void readPathWorksWithDefaultFfmKeyspace() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 0, "noeviction", 5, 5, 5);
            try {
                db.bindToCurrentThread();
                YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
                try (FastTestClient client = new FastTestClient(processor)) {
                    Assert.assertTrue(client.execute(cmd("SET", "k", "v")) instanceof ReplySimpleString);
                    Assert.assertTrue(db.memory().memoryStats().keysStoredOffHeap());

                    ReplyBulkString v = (ReplyBulkString) client.execute(cmd("GET", "k"));
                    Assert.assertEquals("v", v.asString());

                    ReplyInteger exists = (ReplyInteger) client.execute(cmd("EXISTS", "k"));
                    Assert.assertEquals(1L, exists.value());

                    ReplySimpleString type = (ReplySimpleString) client.execute(cmd("TYPE", "k"));
                    Assert.assertEquals("string", type.value());

                    ReplyInteger ttl = (ReplyInteger) client.execute(cmd("TTL", "k"));
                    Assert.assertEquals(-1L, ttl.value());
                }
            } finally {
                db.shutdown();
            }
        }
    }
}
