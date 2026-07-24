package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplySimpleString;

import static yier.bubu.redis.testutil.TestBytes.cmd;
import static yier.bubu.redis.testutil.TestDbs.createFfmDb;

public class OffHeapKeysZeroCopyReadPathTest {
    @Test
    public void readPathWorksWithDefaultFfmKeyspace() {
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
