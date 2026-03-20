package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.db.memory.unsafe.YierdisUnsafeOffHeapAllocator;
import yier.bubu.redis.offheap.api.OffHeapSlice;
import yier.bubu.redis.ops.result.BulkStringMapPairs;
import yier.bubu.redis.ops.result.BulkStringSequence;
import yier.bubu.redis.ops.result.BulkStringSink;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static yier.bubu.redis.testutil.TestBytes.b;

public class OffHeapCollectionReadStreamingTest {
    @Test
    public void lrangeStreamsOffHeapSlices() {
        withDb(db -> {
            db.rpush(b("list"), List.of(b("a"), b("b"), b("c")));

            RecordingBulkSequenceOutput out = new RecordingBulkSequenceOutput();
            BulkStringSequence seq = db.reads().lists().lrange(b("list"), 0, -1);
            Assert.assertEquals(3, seq.count());

            seq.emitTo(out);
            Assert.assertTrue(out.sawOffHeapSlice());
            Assert.assertEquals(List.of("a", "b", "c"), out.strings());
        });
    }

    @Test
    public void smembersStreamsOffHeapSlices() {
        withDb(db -> {
            db.sadd(b("set"), List.of(b("alpha"), b("beta")));

            RecordingBulkSequenceOutput out = new RecordingBulkSequenceOutput();
            BulkStringSequence seq = db.reads().sets().smembers(b("set"));
            Assert.assertEquals(2, seq.count());

            seq.emitTo(out);
            Assert.assertTrue(out.sawOffHeapSlice());
            Assert.assertTrue(out.strings().contains("alpha"));
            Assert.assertTrue(out.strings().contains("beta"));
        });
    }

    @Test
    public void hgetallStreamsOffHeapSlices() {
        withDb(db -> {
            db.hset(b("hash"), List.of(b("field"), b("value")));

            RecordingBulkSequenceOutput out = new RecordingBulkSequenceOutput();
            BulkStringMapPairs pairs = db.reads().hashes().hgetall(b("hash"));
            Assert.assertEquals(1, pairs.pairCount());

            pairs.emitPairsTo(out);
            Assert.assertTrue(out.sawOffHeapSlice());
            Assert.assertEquals(List.of("field", "value"), out.strings());
        });
    }

    @Test
    public void zrangeStreamsOffHeapSlices() {
        withDb(db -> {
            db.zadd(b("z"), List.of(b("1"), b("m1"), b("2"), b("m2")));

            RecordingBulkSequenceOutput out = new RecordingBulkSequenceOutput();
            BulkStringSequence seq = db.reads().zsets().zrange(b("z"), 0, -1, false);
            Assert.assertEquals(2, seq.count());

            seq.emitTo(out);
            Assert.assertTrue(out.sawOffHeapSlice());
            Assert.assertEquals(List.of("m1", "m2"), out.strings());
        });
    }

    private static void withDb(DbConsumer consumer) {
        YierdisUnsafeOffHeapAllocator allocator = new YierdisUnsafeOffHeapAllocator(0);
        YierdisDb db = new YierdisDb(allocator, true, false, 0, "noeviction", 5, 5, 5);
        try {
            db.bindToCurrentThread();
            consumer.accept(db);
        } finally {
            db.shutdown();
        }
    }

    @FunctionalInterface
    private interface DbConsumer {
        void accept(YierdisDb db);
    }

    private static final class RecordingBulkSequenceOutput implements BulkStringSink {
        private final List<String> values = new ArrayList<>();
        private boolean sawOffHeapSlice;

        @Override
        public void bulkString(byte[] data) {
            sawOffHeapSlice = false;
            values.add(data == null ? null : new String(data, StandardCharsets.UTF_8));
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            sawOffHeapSlice = false;
            values.add(data == null ? null : new String(data, off, len, StandardCharsets.UTF_8));
        }

        @Override
        public void bulkString(BytesSlice slice) {
            if (slice == null) {
                values.add(null);
                return;
            }
            if (slice instanceof OffHeapSlice) {
                sawOffHeapSlice = true;
            }
            byte[] bytes = new byte[slice.length()];
            slice.getBytes(0, bytes, 0, bytes.length);
            values.add(new String(bytes, StandardCharsets.UTF_8));
        }

        @Override
        public void bulkStringLongAscii(long value) {
            sawOffHeapSlice = false;
            values.add(Long.toString(value));
        }

        private boolean sawOffHeapSlice() {
            return sawOffHeapSlice;
        }

        private List<String> strings() {
            return values;
        }
    }
}
