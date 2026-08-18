package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.result.ByteMapSource;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;
import yier.bubu.redis.storage.api.result.ByteValueSink;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class NativeCollectionReadStreamingTest {
    @Test
    public void lrangeStreamsBytesSlices() {
        withDb(db -> {
            db.lists().rpush(b("list"), List.of(b("a"), b("b"), b("c"))).value();

            RecordingBulkSequenceOutput out = new RecordingBulkSequenceOutput();
            ByteSequenceSource seq = db.lists().lrange(b("list"), 0, -1);
            Assert.assertEquals(3, seq.elementCount());

            seq.emitTo(out);
            Assert.assertTrue(out.sawBytesSlice());
            Assert.assertEquals(List.of("a", "b", "c"), out.strings());
        });
    }

    @Test
    public void smembersStreamsBytesSlices() {
        withDb(db -> {
            db.sets().sadd(b("set"), List.of(b("alpha"), b("beta"))).value();

            RecordingBulkSequenceOutput out = new RecordingBulkSequenceOutput();
            ByteSequenceSource seq = db.sets().smembers(b("set"));
            Assert.assertEquals(2, seq.elementCount());

            seq.emitTo(out);
            Assert.assertTrue(out.sawBytesSlice());
            Assert.assertTrue(out.strings().contains("alpha"));
            Assert.assertTrue(out.strings().contains("beta"));
        });
    }

    @Test
    public void hgetallStreamsBytesSlices() {
        withDb(db -> {
            db.hashes().hset(b("hash"), List.of(b("field"), b("value"))).value();

            RecordingBulkSequenceOutput out = new RecordingBulkSequenceOutput();
            ByteMapSource pairs = db.hashes().hgetall(b("hash"));
            Assert.assertEquals(1, pairs.pairCount());

            pairs.emitPairsTo(out);
            Assert.assertTrue(out.sawBytesSlice());
            Assert.assertEquals(List.of("field", "value"), out.strings());
        });
    }

    @Test
    public void zrangeStreamsBytesSlices() {
        withDb(db -> {
            db.zsets().zadd(b("z"), List.of(b("1"), b("m1"), b("2"), b("m2"))).value();

            RecordingBulkSequenceOutput out = new RecordingBulkSequenceOutput();
            ByteSequenceSource seq = db.zsets().zrange(b("z"), 0, -1, false);
            Assert.assertEquals(2, seq.elementCount());

            seq.emitTo(out);
            Assert.assertTrue(out.sawBytesSlice());
            Assert.assertEquals(List.of("m1", "m2"), out.strings());
        });
    }

    private static void withDb(DbConsumer consumer) {
        try (TestBackend runtime = TestBackend.open("db")) {
            YierdisDb db = TestDbSupport.open(runtime, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
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

    private static final class RecordingBulkSequenceOutput implements ByteValueSink {
        private final List<String> values = new ArrayList<>();
        private boolean sawBytesSlice;

        @Override
        public void value(byte[] data) {
            sawBytesSlice = false;
            values.add(data == null ? null : new String(data, StandardCharsets.UTF_8));
        }

        @Override
        public void value(byte[] data, int off, int len) {
            sawBytesSlice = false;
            values.add(data == null ? null : new String(data, off, len, StandardCharsets.UTF_8));
        }

        @Override
        public void value(BytesSlice slice) {
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
        public void longAscii(long value) {
            sawBytesSlice = false;
            values.add(Long.toString(value));
        }

        @Override
        public void nullValue() {
            value((byte[]) null);
        }

        private boolean sawBytesSlice() {
            return sawBytesSlice;
        }

        private List<String> strings() {
            return values;
        }
    }
}
