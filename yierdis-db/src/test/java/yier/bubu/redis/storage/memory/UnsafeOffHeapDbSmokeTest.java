package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.SetMode;

import java.util.List;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class UnsafeOffHeapDbSmokeTest {
    @Test
    public void offHeapCompositeTypesWorkAndShutdownDoesNotLeak() {
        try (TestBackend runtime = TestBackend.open("db")) {
            YierdisDb db = TestDbSupport.open(runtime, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
            try {
                db.bindToCurrentThread();
                Assert.assertTrue(db.strings().setString(b("s"), b("v"), SetMode.NORMAL, null).value());
                Assert.assertArrayEquals(b("v"), OwnedReplyValueAssertions.stringValue(db.strings(), b("s")));

                Assert.assertEquals(3L, (long) db.lists().rpush(b("l"), List.of(b("a"), b("b"), b("c"))).value());
                var range = db.lists().lrange(b("l"), 0, -1);
                Assert.assertEquals(3, range.elementCount());
                RecordingBulkSequenceOutput rangeOut = new RecordingBulkSequenceOutput();
                range.emitTo(rangeOut);
                Assert.assertEquals(3, rangeOut.values.size());
                Assert.assertArrayEquals(b("a"), rangeOut.values.get(0));
                Assert.assertArrayEquals(b("b"), rangeOut.values.get(1));
                Assert.assertArrayEquals(b("c"), rangeOut.values.get(2));

                Assert.assertEquals(2L, (long) db.hashes().hset(b("h"), List.of(b("f1"), b("v1"), b("f2"), b("v2"))).value());
                Assert.assertArrayEquals(b("v1"), OwnedReplyValueAssertions.bytes(db.hashes().hget(b("h"), b("f1"))));
                Assert.assertEquals(2, db.hashes().hgetall(b("h")).pairCount());

                Assert.assertEquals(3L, (long) db.sets().sadd(b("set"), List.of(b("x"), b("y"), b("z"))).value());
                Assert.assertTrue(db.sets().sismember(b("set"), b("y")));
                Assert.assertEquals(3, db.sets().scard(b("set")));

                Assert.assertEquals(3L, (long) db.zsets().zadd(b("z"), List.of(
                        b("1"), b("a"),
                        b("1"), b("b"),
                        b("0"), b("c")
                )).value());
                var zrange = db.zsets().zrange(b("z"), 0, -1, false);
                Assert.assertEquals(3, zrange.elementCount());
                RecordingBulkSequenceOutput zrangeOut = new RecordingBulkSequenceOutput();
                zrange.emitTo(zrangeOut);
                Assert.assertEquals(3, zrangeOut.values.size());
                Assert.assertArrayEquals(b("c"), zrangeOut.values.get(0));
                Assert.assertArrayEquals(b("a"), zrangeOut.values.get(1));
                Assert.assertArrayEquals(b("b"), zrangeOut.values.get(2));
            } finally {
                db.shutdown();
            }
        }
    }

    private static final class RecordingBulkSequenceOutput implements yier.bubu.redis.storage.api.result.ByteValueSink {
        private final java.util.List<byte[]> values = new java.util.ArrayList<>();

        @Override
        public void value(byte[] data) {
            values.add(copy(data, 0, data == null ? 0 : data.length));
        }

        @Override
        public void value(byte[] data, int off, int len) {
            values.add(copy(data, off, len));
        }

        @Override
        public void value(yier.bubu.redis.bytes.BytesSlice slice) {
            if (slice == null) {
                values.add(null);
                return;
            }
            byte[] data = new byte[slice.length()];
            slice.getBytes(0, data, 0, data.length);
            values.add(data);
        }

        @Override
        public void longAscii(long value) {
            values.add(Long.toString(value).getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        }

        @Override
        public void nullValue() {
            values.add(null);
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
