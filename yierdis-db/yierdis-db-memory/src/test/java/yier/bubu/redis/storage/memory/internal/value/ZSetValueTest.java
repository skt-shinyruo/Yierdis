package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.NativeAllocator;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.api.result.BulkStringSink;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ZSetValueTest {
    @Test
    public void packedZSetKeepsScoreOrderingAndSupportsUpdates() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("zset-test");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            ZSetValue zv = new ZSetValue(allocator);
            try {
                Assert.assertEquals(ValueEncoding.ZSET_PACKED, zv.encoding());

                List<byte[]> args = Arrays.asList(
                        b("1"), b("a"),
                        b("1"), b("b"),
                        b("0"), b("c")
                );
                Assert.assertEquals(3, zv.zaddMany(args));

                List<byte[]> range = zv.zrange(0, -1, false);
                Assert.assertEquals(3, range.size());
                Assert.assertArrayEquals(b("c"), range.get(0));
                Assert.assertArrayEquals(b("a"), range.get(1));
                Assert.assertArrayEquals(b("b"), range.get(2));

                Assert.assertEquals(0, zv.zaddMany(Arrays.asList(b("2"), b("a"))));
                List<byte[]> range2 = zv.zrange(0, -1, false);
                Assert.assertEquals(3, range2.size());
                Assert.assertArrayEquals(b("c"), range2.get(0));
                Assert.assertArrayEquals(b("b"), range2.get(1));
                Assert.assertArrayEquals(b("a"), range2.get(2));

                Assert.assertEquals(1, zv.zrem(List.of(b("b"))));
                Assert.assertEquals(2, zv.size());
            } finally {
                zv.close();
            }
        }
    }

    @Test
    public void zsetUpgradesAfterTooManyEntries() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("zset-test");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            ZSetValue zv = new ZSetValue(allocator);
            try {
                ArrayList<byte[]> pairs = new ArrayList<>();
                for (int i = 0; i < 200; i++) {
                    pairs.add(b(Integer.toString(i)));
                    pairs.add(b("m" + i));
                }
                Assert.assertEquals(200, zv.zaddMany(pairs));
                Assert.assertEquals(200, zv.size());
                Assert.assertEquals(ValueEncoding.ZSET_SKIPLIST, zv.encoding());
            } finally {
                zv.close();
            }
        }
    }

    @Test
    public void skiplistRangeRemovalUsesMeasuredNativeMemberLookup() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("zset-remove-skiplist");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            ZSetValue zv = new ZSetValue(allocator);
            try {
                ArrayList<byte[]> pairs = new ArrayList<>();
                for (int i = 0; i < 200; i++) {
                    pairs.add(b(Integer.toString(i)));
                    pairs.add(b("m" + i));
                }
                Assert.assertEquals(200, zv.zaddMany(pairs));
                Assert.assertEquals(ValueEncoding.ZSET_SKIPLIST, zv.encoding());

                Assert.assertEquals(100, zv.countRemovalsByRank(50, 149));
                Assert.assertEquals(100, zv.zremrangeByRank(50, 149));
                Assert.assertEquals(100, zv.size());
                Assert.assertEquals(50, zv.countRemovalsByScore(0, false, 49, false));
                Assert.assertEquals(50, zv.zremrangeByScore(0, false, 49, false));
                Assert.assertEquals(50, zv.size());
            } finally {
                zv.close();
            }
        }
    }

    @Test
    public void preparedCopyHeapUpperBoundCoversListpackToSkiplistUpgrade() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("zset-prepared-heap");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            ZSetValue source = new ZSetValue(allocator);
            ZSetValue replacement = new ZSetValue(allocator);
            try {
                List<byte[]> sourcePairs = List.of(
                        b("1"), b("alpha"), b("2"), b("beta"), b("3"), b("gamma")
                );
                source.zaddMany(sourcePairs);
                List<byte[]> addition = List.of(b("4"), new byte[256]);
                long upperBound = source.preparedCopyHeapUpperBound(addition);

                replacement.zaddMany(sourcePairs);
                replacement.prepareAdd(addition);

                Assert.assertTrue(replacement.heapEstimatedBytes() <= upperBound);
            } finally {
                replacement.close();
                source.close();
            }
        }
    }

    @Test
    public void packedZSetStreamsMembersThroughNativeBytesSlice() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("zset-stream-packed");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            ZSetValue zv = new ZSetValue(allocator);
            try {
                Assert.assertEquals(2, zv.zaddMany(List.of(b("1"), b("m1"), b("2"), b("m2"))));
                RecordingSink out = new RecordingSink();

                zv.zrangeWriteTo(0, -1, false, out);

                Assert.assertEquals(List.of("m1", "m2"), out.values);
                Assert.assertTrue(out.sawNativeBytesSlice);
            } finally {
                zv.close();
            }
        }
    }

    @Test
    public void skiplistZSetStreamsMembersThroughNativeBytesSlice() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("zset-stream-skiplist");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            ZSetValue zv = new ZSetValue(allocator);
            try {
                ArrayList<byte[]> pairs = new ArrayList<>();
                for (int i = 0; i < 200; i++) {
                    pairs.add(b(Integer.toString(i)));
                    pairs.add(b("m" + i));
                }
                Assert.assertEquals(200, zv.zaddMany(pairs));
                RecordingSink out = new RecordingSink();

                zv.zrangeWriteTo(0, 1, false, out);

                Assert.assertEquals(List.of("m0", "m1"), out.values);
                Assert.assertTrue(out.sawNativeBytesSlice);
            } finally {
                zv.close();
            }
        }
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    private static final class RecordingSink implements BulkStringSink {
        private final ArrayList<String> values = new ArrayList<>();
        private boolean sawNativeBytesSlice;

        @Override
        public void bulkString(byte[] data) {
            values.add(data == null ? null : new String(data, StandardCharsets.US_ASCII));
            sawNativeBytesSlice = false;
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            values.add(data == null ? null : new String(data, off, len, StandardCharsets.US_ASCII));
            sawNativeBytesSlice = false;
        }

        @Override
        public void bulkString(BytesSlice slice) {
            if (slice == null) {
                values.add(null);
                return;
            }
            byte[] bytes = new byte[slice.length()];
            slice.getBytes(0, bytes, 0, bytes.length);
            values.add(new String(bytes, StandardCharsets.US_ASCII));
            sawNativeBytesSlice = slice instanceof NativeBytesSlice;
        }

        @Override
        public void bulkStringLongAscii(long value) {
            values.add(Long.toString(value));
            sawNativeBytesSlice = false;
        }
    }
}
