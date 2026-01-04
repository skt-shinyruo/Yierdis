package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.offheap.netty.YierdisNettyOffHeapAllocator;
import yier.bubu.redis.db.offheap.unsafe.YierdisUnsafeOffHeapAllocator;
import yier.bubu.redis.protocol.RespInteger;
import yier.bubu.redis.testutil.FastTestClient;

import java.util.Arrays;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestBytes.cmd;

public class OffHeapLeakRegressionTest {
    @Test
    public void nettyOffHeapEvictionAndExpireDoNotLeak() {
        YierdisNettyOffHeapAllocator allocator = new YierdisNettyOffHeapAllocator(0);
        YierdisDb db = new YierdisDb(allocator, 2500, "allkeys-random", 5);
        try {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                byte[] v1600 = repeat((byte) 'x', 1600);

                Assert.assertTrue(client.execute(List.of(b("SET"), b("a"), v1600)) instanceof yier.bubu.redis.protocol.RespSimpleString);
                long usedAfterA = allocator.usedBytes();
                Assert.assertTrue(usedAfterA > 0);

                Assert.assertTrue(client.execute(List.of(b("SET"), b("b"), v1600)) instanceof yier.bubu.redis.protocol.RespSimpleString);
                RespInteger exists = (RespInteger) client.execute(cmd("EXISTS", "a", "b"));
                Assert.assertEquals(1, exists.value());
                Assert.assertTrue("eviction should free some off-heap bytes", allocator.usedBytes() <= usedAfterA);

                // Expire path: ensure payload is released.
                Assert.assertTrue(client.execute(cmd("SET", "e", "v")) instanceof yier.bubu.redis.protocol.RespSimpleString);
                Assert.assertEquals(1L, ((RespInteger) client.execute(cmd("EXPIRE", "e", "0"))).value());
                Assert.assertEquals(-2L, ((RespInteger) client.execute(cmd("TTL", "e"))).value());
            }
        } finally {
            db.shutdown();
            Assert.assertEquals(0L, allocator.usedBytes());
        }
    }

    @Test
    public void unsafeOffHeapEvictionDeleteAndExpireDoNotLeak() {
        YierdisUnsafeOffHeapAllocator allocator = new YierdisUnsafeOffHeapAllocator(0);
        YierdisDb db = new YierdisDb(allocator, 4500, "allkeys-random", 5);
        try {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);
            try (FastTestClient client = new FastTestClient(processor)) {
                byte[] v1600 = repeat((byte) 'x', 1600);

                Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("a"), v1600)) instanceof yier.bubu.redis.protocol.RespSimpleString);
                Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("b"), v1600)) instanceof yier.bubu.redis.protocol.RespSimpleString);
                Assert.assertTrue(client.execute(Arrays.asList(b("SET"), b("c"), v1600)) instanceof yier.bubu.redis.protocol.RespSimpleString);

                RespInteger exists = (RespInteger) client.execute(cmd("EXISTS", "a", "b", "c"));
                Assert.assertEquals(2, exists.value());

                // Delete + expire: cover more释放路径。
                client.execute(cmd("DEL", "a", "b", "c"));
                Assert.assertTrue(client.execute(cmd("SET", "e", "v")) instanceof yier.bubu.redis.protocol.RespSimpleString);
                Assert.assertEquals(1L, ((RespInteger) client.execute(cmd("EXPIRE", "e", "0"))).value());
                Assert.assertEquals(-2L, ((RespInteger) client.execute(cmd("TTL", "e"))).value());
            }
        } finally {
            db.shutdown();
            Assert.assertEquals(0L, allocator.usedBytes());
        }
    }

    private static byte[] repeat(byte b, int len) {
        byte[] out = new byte[len];
        Arrays.fill(out, b);
        return out;
    }
}

