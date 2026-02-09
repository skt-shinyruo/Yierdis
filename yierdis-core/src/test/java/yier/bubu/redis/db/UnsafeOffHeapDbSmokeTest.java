package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.offheap.unsafe.YierdisUnsafeOffHeapAllocator;
import yier.bubu.redis.ops.SetMode;

import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;

public class UnsafeOffHeapDbSmokeTest {
    @Test
    public void offHeapCompositeTypesWorkAndShutdownDoesNotLeak() {
            YierdisUnsafeOffHeapAllocator allocator = new YierdisUnsafeOffHeapAllocator(0);
            YierdisDb db = new YierdisDb(allocator);
            try {
                db.bindToCurrentThread();
                Assert.assertTrue(db.setString(b("s"), b("v"), SetMode.NORMAL, null));
                Assert.assertArrayEquals(b("v"), db.getStringBytes(b("s")));

            Assert.assertEquals(3, db.rpush(b("l"), List.of(b("a"), b("b"), b("c"))));
            List<byte[]> range = db.lrange(b("l"), 0, -1);
            Assert.assertEquals(3, range.size());
            Assert.assertArrayEquals(b("a"), range.get(0));
            Assert.assertArrayEquals(b("b"), range.get(1));
            Assert.assertArrayEquals(b("c"), range.get(2));

            Assert.assertEquals(2, db.hset(b("h"), List.of(b("f1"), b("v1"), b("f2"), b("v2"))));
            Assert.assertArrayEquals(b("v1"), db.hget(b("h"), b("f1")));
            Assert.assertEquals(2, db.values().hashes().hgetallPairCount(b("h")));

            Assert.assertEquals(3, db.sadd(b("set"), List.of(b("x"), b("y"), b("z"))));
            Assert.assertTrue(db.sismember(b("set"), b("y")));
            Assert.assertEquals(3, db.scard(b("set")));

            Assert.assertEquals(3, db.zadd(b("z"), List.of(
                    b("1"), b("a"),
                    b("1"), b("b"),
                    b("0"), b("c")
            )));
            List<byte[]> zrange = db.zrange(b("z"), 0, -1, false);
            Assert.assertEquals(3, zrange.size());
            Assert.assertArrayEquals(b("c"), zrange.get(0));
            Assert.assertArrayEquals(b("a"), zrange.get(1));
            Assert.assertArrayEquals(b("b"), zrange.get(2));
        } finally {
            // Closes the allocator; will throw on leaks.
            db.shutdown();
        }
    }
}
