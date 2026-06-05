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
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.result.BulkStringMapPairs;
import yier.bubu.redis.storage.api.result.BulkStringSequence;
import yier.bubu.redis.storage.api.result.BulkStringSink;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class OffHeapCollectionReadStreamingTest {
    @Test
    public void lrangeStreamsBytesSlices() {
        withDb(db -> {
            db.writes().lists().rpush(b("list"), List.of(b("a"), b("b"), b("c"))).value();

            RecordingBulkSequenceOutput out = new RecordingBulkSequenceOutput();
            BulkStringSequence seq = db.reads().lists().lrange(b("list"), 0, -1);
            Assert.assertEquals(3, seq.count());

            seq.emitTo(out);
            Assert.assertTrue(out.sawBytesSlice());
            Assert.assertEquals(List.of("a", "b", "c"), out.strings());
        });
    }

    @Test
    public void smembersStreamsBytesSlices() {
        withDb(db -> {
            db.writes().sets().sadd(b("set"), List.of(b("alpha"), b("beta"))).value();

            RecordingBulkSequenceOutput out = new RecordingBulkSequenceOutput();
            BulkStringSequence seq = db.reads().sets().smembers(b("set"));
            Assert.assertEquals(2, seq.count());

            seq.emitTo(out);
            Assert.assertTrue(out.sawBytesSlice());
            Assert.assertTrue(out.strings().contains("alpha"));
            Assert.assertTrue(out.strings().contains("beta"));
        });
    }

    @Test
    public void hgetallStreamsBytesSlices() {
        withDb(db -> {
            db.writes().hashes().hset(b("hash"), List.of(b("field"), b("value"))).value();

            RecordingBulkSequenceOutput out = new RecordingBulkSequenceOutput();
            BulkStringMapPairs pairs = db.reads().hashes().hgetall(b("hash"));
            Assert.assertEquals(1, pairs.pairCount());

            pairs.emitPairsTo(out);
            Assert.assertTrue(out.sawBytesSlice());
            Assert.assertEquals(List.of("field", "value"), out.strings());
        });
    }

    @Test
    public void zrangeStreamsBytesSlices() {
        withDb(db -> {
            db.writes().zsets().zadd(b("z"), List.of(b("1"), b("m1"), b("2"), b("m2"))).value();

            RecordingBulkSequenceOutput out = new RecordingBulkSequenceOutput();
            BulkStringSequence seq = db.reads().zsets().zrange(b("z"), 0, -1, false);
            Assert.assertEquals(2, seq.count());

            seq.emitTo(out);
            Assert.assertTrue(out.sawBytesSlice());
            Assert.assertEquals(List.of("m1", "m2"), out.strings());
        });
    }

    private static void withDb(DbConsumer consumer) {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("db")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 0, "noeviction", 5, 5, 5);
            try {
                db.bindToCurrentThread();
                consumer.accept(db);
            } finally {
                db.shutdown();
            }
        }
    }

    @FunctionalInterface
    private interface DbConsumer {
        void accept(YierdisDb db);
    }

    private static final class RecordingBulkSequenceOutput implements BulkStringSink {
        private final List<String> values = new ArrayList<>();
        private boolean sawBytesSlice;

        @Override
        public void bulkString(byte[] data) {
            sawBytesSlice = false;
            values.add(data == null ? null : new String(data, StandardCharsets.UTF_8));
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            sawBytesSlice = false;
            values.add(data == null ? null : new String(data, off, len, StandardCharsets.UTF_8));
        }

        @Override
        public void bulkString(BytesSlice slice) {
            if (slice == null) {
                values.add(null);
                return;
            }
            sawBytesSlice = true;
            byte[] bytes = new byte[slice.length()];
            slice.getBytes(0, bytes, 0, bytes.length);
            values.add(new String(bytes, StandardCharsets.UTF_8));
        }

        @Override
        public void bulkStringLongAscii(long value) {
            sawBytesSlice = false;
            values.add(Long.toString(value));
        }

        private boolean sawBytesSlice() {
            return sawBytesSlice;
        }

        private List<String> strings() {
            return values;
        }
    }
}
