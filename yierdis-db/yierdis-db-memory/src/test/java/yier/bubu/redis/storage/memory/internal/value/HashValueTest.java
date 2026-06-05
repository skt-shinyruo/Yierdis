package yier.bubu.redis.storage.memory.internal.value;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.memory.internal.entry.HashRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;

import java.util.ArrayList;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class HashValueTest {
    @Test
    public void rootCreatedPackedHashStoresFieldsAndValuesAsNativeBytesAndStreamsNativeSlices() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("hash-native-packed-bytes");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             HashRoot root = new HashRoot(allocator)) {
            ValueHandle handle = root.create();

            root.hsetMany(handle, List.of(bytes("field"), bytes("value")));

            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.HASH_FIELD_BYTES));
            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.HASH_VALUE_BYTES));
            RecordingBulkStringSink out = new RecordingBulkStringSink();
            root.hgetallPairsInto(handle, out);
            Assert.assertTrue(out.sawNativeBytesSlice());
            Assert.assertEquals(List.of("field", "value"), out.strings());
        }
    }

    @Test
    public void packedHashSupportsUpdateAndDeleteWithRepacking() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("hash-test");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            HashValue hv = new HashValue(allocator);
            try {
                Assert.assertEquals(ValueEncoding.HASH_PACKED, hv.encoding());

                byte[] f1 = new byte[]{0, 'f', 1};
                byte[] v1 = new byte[]{'v'};
                byte[] f2 = new byte[]{'k'};
                byte[] v2 = new byte[]{'\n', 0, (byte) 0xFF};

                Assert.assertEquals(1, hv.hset(f1, v1));
                Assert.assertEquals(1, hv.hset(f2, v2));
                Assert.assertEquals(2, hv.size());

                Assert.assertArrayEquals(v1, hv.hget(f1));
                Assert.assertArrayEquals(v2, hv.hget(f2));

                byte[] v1Longer = new byte[]{'v', '0', '-', 'n', 'e', 'w'};
                Assert.assertEquals(0, hv.hset(f1, v1Longer));
                Assert.assertArrayEquals(v1Longer, hv.hget(f1));
                Assert.assertArrayEquals(v2, hv.hget(f2));

                Assert.assertEquals(1, hv.hdel(List.of(f2)));
                Assert.assertEquals(1, hv.size());
                Assert.assertNull(hv.hget(f2));
                Assert.assertArrayEquals(v1Longer, hv.hget(f1));

                Assert.assertEquals(1, hv.hdel(Arrays.asList(f1, f1)));
                Assert.assertEquals(0, hv.size());
                Assert.assertNull(hv.hget(f1));
            } finally {
                hv.close();
            }
        }
    }

    @Test
    public void hashConvertsToHashTableAfterTooManyFields() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("hash-test");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            HashValue hv = new HashValue(allocator);
            try {
                int added = 0;
                for (int i = 0; i < 600; i++) {
                    byte[] f = ("f" + i).getBytes(StandardCharsets.US_ASCII);
                    byte[] v = ("v" + i).getBytes(StandardCharsets.US_ASCII);
                    added += hv.hset(f, v);
                }
                Assert.assertEquals(600, added);
                Assert.assertEquals(600, hv.size());
                Assert.assertEquals(ValueEncoding.HASH_HT, hv.encoding());
                Assert.assertArrayEquals("v0".getBytes(StandardCharsets.US_ASCII), hv.hget("f0".getBytes(StandardCharsets.US_ASCII)));
                Assert.assertArrayEquals("v599".getBytes(StandardCharsets.US_ASCII), hv.hget("f599".getBytes(StandardCharsets.US_ASCII)));
            } finally {
                hv.close();
            }
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static final class RecordingBulkStringSink implements BulkStringSink {
        private final List<String> values = new ArrayList<>();
        private boolean sawNativeBytesSlice;

        @Override
        public void bulkString(byte[] data) {
            sawNativeBytesSlice = false;
            values.add(data == null ? null : new String(data, StandardCharsets.US_ASCII));
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            sawNativeBytesSlice = false;
            values.add(data == null ? null : new String(data, off, len, StandardCharsets.US_ASCII));
        }

        @Override
        public void bulkString(BytesSlice slice) {
            if (slice == null) {
                values.add(null);
                return;
            }
            sawNativeBytesSlice = slice instanceof NativeBytesSlice;
            byte[] data = new byte[slice.length()];
            slice.getBytes(0, data, 0, data.length);
            values.add(new String(data, StandardCharsets.US_ASCII));
        }

        @Override
        public void bulkStringLongAscii(long value) {
            sawNativeBytesSlice = false;
            values.add(Long.toString(value));
        }

        private boolean sawNativeBytesSlice() {
            return sawNativeBytesSlice;
        }

        private List<String> strings() {
            return values;
        }
    }
}
