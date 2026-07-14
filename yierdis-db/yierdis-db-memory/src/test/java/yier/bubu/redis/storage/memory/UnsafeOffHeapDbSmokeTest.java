package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;

import java.util.List;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class UnsafeOffHeapDbSmokeTest {
    @Test
    public void offHeapCompositeTypesWorkAndShutdownDoesNotLeak() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
            try {
                db.bindToCurrentThread();
                Assert.assertTrue(db.writes().strings().setString(b("s"), b("v"), SetMode.NORMAL, null).value());
                Assert.assertArrayEquals(b("v"), db.reads().strings().getStringBytes(b("s")));

                Assert.assertEquals(3L, (long) db.writes().lists().rpush(b("l"), List.of(b("a"), b("b"), b("c"))).value());
                var range = db.reads().lists().lrange(b("l"), 0, -1);
                Assert.assertEquals(3, range.count());
                RecordingBulkSequenceOutput rangeOut = new RecordingBulkSequenceOutput();
                range.emitTo(rangeOut);
                Assert.assertEquals(3, rangeOut.values.size());
                Assert.assertArrayEquals(b("a"), rangeOut.values.get(0));
                Assert.assertArrayEquals(b("b"), rangeOut.values.get(1));
                Assert.assertArrayEquals(b("c"), rangeOut.values.get(2));

                Assert.assertEquals(2L, (long) db.writes().hashes().hset(b("h"), List.of(b("f1"), b("v1"), b("f2"), b("v2"))).value());
                Assert.assertArrayEquals(b("v1"), OwnedReplyValueAssertions.bytes(db.reads().hashes().hget(b("h"), b("f1"))));
                Assert.assertEquals(2, db.reads().hashes().hgetall(b("h")).pairCount());

                Assert.assertEquals(3L, (long) db.writes().sets().sadd(b("set"), List.of(b("x"), b("y"), b("z"))).value());
                Assert.assertTrue(db.reads().sets().sismember(b("set"), b("y")));
                Assert.assertEquals(3, db.reads().sets().scard(b("set")));

                Assert.assertEquals(3L, (long) db.writes().zsets().zadd(b("z"), List.of(
                        b("1"), b("a"),
                        b("1"), b("b"),
                        b("0"), b("c")
                )).value());
                var zrange = db.reads().zsets().zrange(b("z"), 0, -1, false);
                Assert.assertEquals(3, zrange.count());
                RecordingBulkSequenceOutput zrangeOut = new RecordingBulkSequenceOutput();
                zrange.emitTo(zrangeOut);
                Assert.assertEquals(3, zrangeOut.values.size());
                Assert.assertArrayEquals(b("c"), zrangeOut.values.get(0));
                Assert.assertArrayEquals(b("a"), zrangeOut.values.get(1));
                Assert.assertArrayEquals(b("b"), zrangeOut.values.get(2));
            } finally {
                db.shutdown();
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    private static final class RecordingBulkSequenceOutput implements yier.bubu.redis.storage.api.result.BulkStringSink {
        private final java.util.List<byte[]> values = new java.util.ArrayList<>();

        @Override
        public void bulkString(byte[] data) {
            values.add(copy(data, 0, data == null ? 0 : data.length));
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            values.add(copy(data, off, len));
        }

        @Override
        public void bulkString(yier.bubu.redis.bytes.BytesSlice slice) {
            if (slice == null) {
                values.add(null);
                return;
            }
            byte[] data = new byte[slice.length()];
            slice.getBytes(0, data, 0, data.length);
            values.add(data);
        }

        @Override
        public void bulkStringLongAscii(long value) {
            values.add(Long.toString(value).getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        }

        private static byte[] copy(byte[] data, int off, int len) {
            if (data == null) {
                return null;
            }
            byte[] copy = new byte[len];
            System.arraycopy(data, off, copy, 0, len);
            return copy;
        }
    }
}
