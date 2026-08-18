package yier.bubu.redis.storage.memory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.result.ByteMapSource;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;
import yier.bubu.redis.storage.api.result.KeyScanWindow;
import yier.bubu.redis.storage.testkit.MaterializingByteValueSink;

public class SemanticResultSourceTest {
    @Test
    public void collectionSourcesExposeRepeatableSemanticLengthsAndReplayBytes() {
        withDb(db -> {
            byte[] binary = new byte[]{0, (byte) 0xFF, '\n'};
            byte[] large = new byte[1_024];
            for (int i = 0; i < large.length; i++) {
                large[i] = (byte) i;
            }

            db.lists().rpush(bytes("list"), List.of(binary, large));
            assertSequence(db.lists().lrange(bytes("list"), 0, -1), List.of(binary, large));
            assertSequence(db.lists().lrange(bytes("missing"), 0, -1), List.of());

            db.hashes().hset(bytes("hash"), List.of(binary, large));
            try (ByteMapSource source = db.hashes().hgetall(bytes("hash"))) {
                Assert.assertEquals(1, source.pairCount());
                Assert.assertEquals(List.of(binary.length, large.length), pairLengths(source));
                Assert.assertEquals(List.of(binary.length, large.length), pairLengths(source));
                RecordingSink sink = new RecordingSink();
                source.emitPairsTo(sink);
                assertByteValues(List.of(binary, large), sink.values());
            }

            byte[] min = Long.toString(Long.MIN_VALUE).getBytes(StandardCharsets.US_ASCII);
            byte[] max = Long.toString(Long.MAX_VALUE).getBytes(StandardCharsets.US_ASCII);
            db.sets().sadd(bytes("set"), List.of(min, max));
            try (ByteSequenceSource source = db.sets().smembers(bytes("set"))) {
                Assert.assertEquals(2, source.elementCount());
                Assert.assertEquals(Set.of(min.length, max.length), Set.copyOf(lengths(source)));
                Assert.assertEquals(Set.of(min.length, max.length), Set.copyOf(lengths(source)));
                RecordingSink sink = new RecordingSink();
                source.emitTo(sink);
                Assert.assertEquals(Set.of(base64(min), base64(max)), base64Set(sink.values()));
            }

            db.zsets().zadd(bytes("zset"), List.of(bytes("1"), binary, bytes("2"), large));
            assertSequence(
                    db.zsets().zrange(bytes("zset"), 0, -1, false),
                    List.of(binary, large)
            );
        });
    }

    @Test
    public void keyWindowsExposeSemanticLengthsWithoutMaterializedResults() {
        withDb(db -> {
            byte[] first = bytes("metric:first");
            byte[] second = bytes("metric:second");
            db.strings().setString(first, bytes("v"), SetMode.NORMAL, null);
            db.strings().setString(second, bytes("v"), SetMode.NORMAL, null);
            Set<String> expected = Set.of(base64(first), base64(second));
            Set<Integer> expectedLengths = Set.of(first.length, second.length);

            try (KeyScanWindow window = db.keyspace().keys(bytes("metric:*"), 16, 0L)) {
                Assert.assertTrue(window.current());
                Assert.assertEquals(2, window.elementCount());
                Assert.assertEquals(expectedLengths, Set.copyOf(lengths(window)));
                Assert.assertEquals(expectedLengths, Set.copyOf(lengths(window)));
                Assert.assertEquals(0L, window.retainedMemoryBytes());
                RecordingSink sink = new RecordingSink();
                window.emitTo(sink);
                Assert.assertEquals(expected, base64Set(sink.values()));
            }

            try (KeyScanWindow window = db.keyspace().scan(ScanCursorV2.start(), bytes("metric:*"), 16)) {
                Assert.assertTrue(window.current());
                Assert.assertEquals(2, window.elementCount());
                Assert.assertEquals(expectedLengths, Set.copyOf(lengths(window)));
                RecordingSink sink = new RecordingSink();
                window.emitTo(sink);
                Assert.assertEquals(expected, base64Set(sink.values()));
            }
        });
    }

    private static void assertSequence(ByteSequenceSource source, List<byte[]> expected) {
        try (source) {
            Assert.assertEquals(expected.size(), source.elementCount());
            Assert.assertEquals(payloadLengths(expected), lengths(source));
            Assert.assertEquals(payloadLengths(expected), lengths(source));
            Assert.assertEquals(0L, source.retainedMemoryBytes());
            RecordingSink sink = new RecordingSink();
            source.emitTo(sink);
            assertByteValues(expected, sink.values());
        }
    }

    private static List<Integer> lengths(ByteSequenceSource source) {
        List<Integer> lengths = new ArrayList<>();
        source.visitElementLengths(lengths::add);
        return lengths;
    }

    private static List<Integer> pairLengths(ByteMapSource source) {
        List<Integer> lengths = new ArrayList<>();
        source.visitPairLengths(lengths::add);
        return lengths;
    }

    private static List<Integer> payloadLengths(List<byte[]> values) {
        return values.stream().map(value -> value == null ? -1 : value.length).toList();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static void assertByteValues(List<byte[]> expected, List<byte[]> actual) {
        Assert.assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            Assert.assertArrayEquals(expected.get(i), actual.get(i));
        }
    }

    private static Set<String> base64Set(List<byte[]> values) {
        return values.stream().map(SemanticResultSourceTest::base64).collect(Collectors.toSet());
    }

    private static String base64(byte[] value) {
        return value == null ? "null" : Base64.getEncoder().encodeToString(value);
    }

    private static void withDb(DbConsumer consumer) {
        try (TestBackend runtime = TestBackend.open("semantic-result-source")) {
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

    private static final class RecordingSink extends MaterializingByteValueSink {
        private final List<byte[]> values = new ArrayList<>();

        @Override
        public void value(byte[] data) {
            values.add(data == null ? null : data.clone());
        }

        private List<byte[]> values() {
            return values;
        }
    }
}
