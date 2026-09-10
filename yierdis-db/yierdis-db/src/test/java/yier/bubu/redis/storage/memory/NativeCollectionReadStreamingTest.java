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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class NativeCollectionReadStreamingTest {
    @Test
    public void lrangeSnapshotSurvivesLaterPops() {
        withDb(db -> {
            db.lists().rpush(b("list"), List.of(b("a"), b("b"), b("c"))).value();

            ByteSequenceSource seq = db.lists().lrange(b("list"), 0, -1);
            Assert.assertEquals(3, seq.elementCount());
            var mutation = db.lists().preparePop(b("list"), 1, true);
            try {
                mutation.commit();
            } finally {
                mutation.close();
            }
            Assert.assertEquals(List.of("a", "b", "c"), strings(seq));
        });
    }

    @Test
    public void smembersSnapshotSurvivesLaterRemovals() {
        withDb(db -> {
            db.sets().sadd(b("set"), List.of(b("alpha"), b("beta"))).value();

            ByteSequenceSource seq = db.sets().smembers(b("set"));
            Assert.assertEquals(2, seq.elementCount());
            db.sets().srem(b("set"), List.of(b("alpha")));
            Assert.assertEquals(Set.of("alpha", "beta"), new HashSet<>(strings(seq)));
        });
    }

    @Test
    public void hgetallSnapshotSurvivesLaterFieldDeletes() {
        withDb(db -> {
            db.hashes().hset(b("hash"), List.of(b("field"), b("value"))).value();

            ByteMapSource pairs = db.hashes().hgetall(b("hash"));
            Assert.assertEquals(1, pairs.pairCount());
            db.hashes().hdel(b("hash"), List.of(b("field")));
            Assert.assertEquals(List.of("field", "value"), pairStrings(pairs));
        });
    }

    @Test
    public void zrangeSnapshotSurvivesLaterRemovals() {
        withDb(db -> {
            db.zsets().zadd(b("z"), List.of(b("1"), b("m1"), b("2"), b("m2"))).value();

            ByteSequenceSource seq = db.zsets().zrange(b("z"), 0, -1, false);
            Assert.assertEquals(2, seq.elementCount());
            db.zsets().zrem(b("z"), List.of(b("m1")));
            Assert.assertEquals(List.of("m1", "m2"), strings(seq));
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

    private static List<String> strings(ByteSequenceSource source) {
        RecordingBulkSequenceOutput out = new RecordingBulkSequenceOutput();
        source.emitTo(out);
        return out.strings();
    }

    private static List<String> pairStrings(ByteMapSource source) {
        RecordingBulkSequenceOutput out = new RecordingBulkSequenceOutput();
        source.emitPairsTo(out);
        return out.strings();
    }

    private static final class RecordingBulkSequenceOutput implements ByteValueSink {
        private final List<String> values = new ArrayList<>();

        @Override
        public void value(byte[] data) {
            values.add(data == null ? null : new String(data, StandardCharsets.UTF_8));
        }

        @Override
        public void value(byte[] data, int off, int len) {
            values.add(data == null ? null : new String(data, off, len, StandardCharsets.UTF_8));
        }

        @Override
        public void value(BytesSlice slice) {
            if (slice == null) {
                values.add(null);
                return;
            }
            byte[] bytes = new byte[slice.length()];
            slice.getBytes(0, bytes, 0, bytes.length);
            values.add(new String(bytes, StandardCharsets.UTF_8));
        }

        @Override
        public void longAscii(long value) {
            values.add(Long.toString(value));
        }

        @Override
        public void nullValue() {
            value((byte[]) null);
        }

        private List<String> strings() {
            return values;
        }
    }
}
