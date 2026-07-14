package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.runtime.embedded.TestCommandProcessors;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyError;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyObject;
import yier.bubu.redis.testutil.ReplySimpleString;

import java.util.Arrays;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;

public class OffHeapLeakRegressionTest {
    private static final int SPAN_VALUE_BYTES = 512 * 1024;

    @Test
    public void ffmEvictionAndExpireDoNotLeak() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(
                    runtime,
                    1_100_000,
                    MaxmemoryPolicy.ALLKEYS_RANDOM,
                    5,
                    5,
                    5
            );
            try {
                db.bindToCurrentThread();
                YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
                try (FastTestClient client = new FastTestClient(processor)) {
                    byte[] spanValue = repeat((byte) 'x', SPAN_VALUE_BYTES);

                    ReplyObject firstSet = client.execute(List.of(b("SET"), b("a"), spanValue));
                    Assert.assertTrue("first SET reply: " + replyDescription(firstSet), firstSet instanceof ReplySimpleString);
                    long usedAfterA = runtime.usedBytes();
                    Assert.assertTrue(usedAfterA > 0);

                    Assert.assertTrue(client.execute(List.of(b("SET"), b("b"), spanValue)) instanceof ReplySimpleString);
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
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(
                    runtime,
                    2_000_000,
                    MaxmemoryPolicy.ALLKEYS_RANDOM,
                    5,
                    5,
                    5
            );
            try {
                db.bindToCurrentThread();
                YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
                try (FastTestClient client = new FastTestClient(processor)) {
                    byte[] spanValue = repeat((byte) 'x', SPAN_VALUE_BYTES);

                    ReplyObject firstSet = client.execute(Arrays.asList(b("SET"), b("a"), spanValue));
                    Assert.assertTrue("first SET reply: " + replyDescription(firstSet), firstSet instanceof ReplySimpleString);
                    ReplyObject secondSet = client.execute(Arrays.asList(b("SET"), b("b"), spanValue));
                    Assert.assertTrue("second SET reply: " + replyDescription(secondSet), secondSet instanceof ReplySimpleString);
                    ReplyObject thirdSet = client.execute(Arrays.asList(b("SET"), b("c"), spanValue));
                    Assert.assertTrue("third SET reply: " + replyDescription(thirdSet), thirdSet instanceof ReplySimpleString);

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

    private static String replyDescription(ReplyObject reply) {
        return reply instanceof ReplyError error ? error.message() : String.valueOf(reply);
    }
}
