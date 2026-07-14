package yier.bubu.redis.storage.memory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.result.BulkStringMapMetrics;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.api.result.KeyScanWindow;
import yier.bubu.redis.storage.api.result.MeasuredBulkStringSequence;

public class MeasuredReplySourceTest {
    @Test
    public void collectionSourcesMeasureAndReplayBinaryLargeAndIntegerBoundaryValues() {
        withDb(db -> {
            byte[] binary = new byte[]{0, (byte) 0xFF, '\n'};
            byte[] large = new byte[1_024];
            for (int i = 0; i < large.length; i++) {
                large[i] = (byte) i;
            }

            db.writes().lists().rpush(bytes("list"), List.of(binary, large));
            assertSequence(
                    db.reads().lists().lrange(bytes("list"), 0, -1),
                    List.of(binary, large)
            );
            assertSequence(db.reads().lists().lrange(bytes("missing"), 0, -1), List.of());

            db.writes().hashes().hset(bytes("hash"), List.of(binary, large));
            try (BulkStringMapMetrics source = db.reads().hashes().hgetall(bytes("hash"))) {
                Assert.assertEquals(1, source.pairCount());
                Assert.assertEquals(encoded(binary) + encoded(large), source.encodedElementBytes());
                RecordingSink sink = new RecordingSink();
                source.emitPairsTo(sink);
                assertByteValues(List.of(binary, large), sink.values());
            }

            byte[] min = Long.toString(Long.MIN_VALUE).getBytes(StandardCharsets.US_ASCII);
            byte[] max = Long.toString(Long.MAX_VALUE).getBytes(StandardCharsets.US_ASCII);
            db.writes().sets().sadd(bytes("set"), List.of(min, max));
            try (MeasuredBulkStringSequence source = db.reads().sets().smembers(bytes("set"))) {
                Assert.assertEquals(2, source.count());
                Assert.assertEquals(encoded(min) + encoded(max), source.encodedElementBytes());
                RecordingSink sink = new RecordingSink();
                source.emitTo(sink);
                Assert.assertEquals(Set.of(encodedValue(min), encodedValue(max)), encodedValueSet(sink.values()));
            }

            db.writes().zsets().zadd(bytes("zset"), List.of(bytes("1"), binary, bytes("2"), large));
            assertSequence(
                    db.reads().zsets().zrange(bytes("zset"), 0, -1, false),
                    List.of(binary, large)
            );
        });
    }

    @Test
    public void keyWindowsMeasureAndReplayOneBoundedDiscoveryWithoutResultLists() {
        withDb(db -> {
            byte[] first = bytes("metric:first");
            byte[] second = bytes("metric:second");
            db.writes().strings().setString(first, bytes("v"), SetMode.NORMAL, null);
            db.writes().strings().setString(second, bytes("v"), SetMode.NORMAL, null);
            long expectedEncoded = encoded(first) + encoded(second);
            Set<String> expected = Set.of(encodedValue(first), encodedValue(second));

            try (KeyScanWindow window = db.reads().keyspace().keys(bytes("metric:*"), 16, 0L)) {
                Assert.assertTrue(window.current());
                Assert.assertEquals(2, window.count());
                Assert.assertEquals(expectedEncoded, window.encodedElementBytes());
                Assert.assertEquals(0L, window.retainedMemoryBytes());
                RecordingSink sink = new RecordingSink();
                window.emitTo(sink);
                Assert.assertEquals(expected, encodedValueSet(sink.values()));
            }

            try (KeyScanWindow window = db.reads().keyspace().scan(ScanCursorV2.start(), bytes("metric:*"), 16)) {
                Assert.assertTrue(window.current());
                Assert.assertEquals(2, window.count());
                Assert.assertEquals(expectedEncoded, window.encodedElementBytes());
                RecordingSink sink = new RecordingSink();
                window.emitTo(sink);
                Assert.assertEquals(expected, encodedValueSet(sink.values()));
            }
        });
    }

    private static void assertSequence(MeasuredBulkStringSequence source, List<byte[]> expected) {
        try (source) {
            Assert.assertEquals(expected.size(), source.count());
            Assert.assertEquals(encodedTotal(expected), source.encodedElementBytes());
            Assert.assertEquals(0L, source.retainedMemoryBytes());
            RecordingSink sink = new RecordingSink();
            source.emitTo(sink);
            assertByteValues(expected, sink.values());
        }
    }

    private static long encodedTotal(List<byte[]> values) {
        long total = 0L;
        for (byte[] value : values) {
            total += encoded(value);
        }
        return total;
    }

    private static long encoded(byte[] value) {
        return 1L + decimalDigits(value.length) + 2L + value.length + 2L;
    }

    private static int decimalDigits(int value) {
        int digits = 1;
        while (value >= 10) {
            value /= 10;
            digits++;
        }
        return digits;
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

    private static Set<String> encodedValueSet(List<byte[]> values) {
        return values.stream().map(MeasuredReplySourceTest::encodedValue).collect(Collectors.toSet());
    }

    private static String encodedValue(byte[] value) {
        return Base64.getEncoder().encodeToString(value);
    }

    private static void withDb(DbConsumer consumer) {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("measured-reply-source")) {
            YierdisDb db = YierdisDb.createWithSharedFfmRuntime(runtime, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
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

    private static final class RecordingSink implements BulkStringSink {
        private final List<byte[]> values = new ArrayList<>();

        @Override
        public void bulkString(byte[] data) {
            values.add(data == null ? null : data.clone());
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            byte[] copy = new byte[len];
            System.arraycopy(data, off, copy, 0, len);
            values.add(copy);
        }

        @Override
        public void bulkString(BytesSlice slice) {
            if (slice == null) {
                values.add(null);
                return;
            }
            byte[] copy = new byte[slice.length()];
            slice.getBytes(0, copy, 0, copy.length);
            values.add(copy);
        }

        @Override
        public void bulkStringLongAscii(long value) {
            values.add(Long.toString(value).getBytes(StandardCharsets.US_ASCII));
        }

        private List<byte[]> values() {
            return values;
        }
    }
}
