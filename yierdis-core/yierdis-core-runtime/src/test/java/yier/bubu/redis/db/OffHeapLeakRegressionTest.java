package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.command.TestCommandProcessors;
import yier.bubu.redis.db.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.util.Arrays;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;

public class OffHeapLeakRegressionTest {
    @Test
    public void ffmEvictionAndExpireDoNotLeak() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 2500, "allkeys-random", 5, 5, 5);
            try {
                db.bindToCurrentThread();
                YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
                try (FastTestClient client = new FastTestClient(processor)) {
                    byte[] v1600 = repeat((byte) 'x', 1600);

                    Assert.assertTrue(client.execute(List.of(b("SET"), b("a"), v1600)) instanceof ReplySimpleString);
                    long usedAfterA = runtime.usedBytes();
                    Assert.assertTrue(usedAfterA > 0);

                    Assert.assertTrue(client.execute(List.of(b("SET"), b("b"), v1600)) instanceof ReplySimpleString);
                    ReplyInteger exists = (ReplyInteger) client.execute(cmd("EXISTS", "a", "b"));
                    Assert.assertEquals(1L, exists.value());
                    Assert.assertTrue("eviction should free some off-heap bytes", runtime.usedBytes() <= usedAfterA);

                    Assert.assertTrue(client.execute(cmd("SET", "e", "v")) instanceof ReplySimpleString);
                    Assert.assertEquals(1L, ((ReplyInteger) client.execute(cmd("EXPIRE", "e", "0"))).value());
                    Assert.assertEquals(-2L, ((ReplyInteger) client.execute(cmd("TTL", "e"))).value());
                }
            } finally {
                db.shutdown();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void ffmEvictionDeleteAndExpireDoNotLeak() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 4500, "allkeys-random", 5, 5, 5);
            try {
                db.bindToCurrentThread();
                YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
                try (FastTestClient client = new FastTestClient(processor)) {
                    byte[] v1600 = repeat((byte) 'x', 1600);

                    Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("a"), v1600)) instanceof ReplySimpleString);
                    Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("b"), v1600)) instanceof ReplySimpleString);
                    Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("c"), v1600)) instanceof ReplySimpleString);

                    ReplyInteger exists = (ReplyInteger) client.execute(cmd("EXISTS", "a", "b", "c"));
                    Assert.assertEquals(2L, exists.value());

                    client.execute(cmd("DEL", "a", "b", "c"));
                    Assert.assertTrue(client.execute(cmd("SET", "e", "v")) instanceof ReplySimpleString);
                    Assert.assertEquals(1L, ((ReplyInteger) client.execute(cmd("EXPIRE", "e", "0"))).value());
                    Assert.assertEquals(-2L, ((ReplyInteger) client.execute(cmd("TTL", "e"))).value());
                }
            } finally {
                db.shutdown();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    private static byte[] repeat(byte b, int len) {
        byte[] out = new byte[len];
        Arrays.fill(out, b);
        return out;
    }
}
