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
import java.util.List;

public class SetValueTest {
    @Test
    public void nativeSetKeepsIntsetMembersAndUpgradesToNativeHashtable() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("set-test");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            SetValue sv = new SetValue(allocator);
            try {
                Assert.assertEquals(ValueEncoding.SET_INTSET, sv.encoding());

                Assert.assertEquals(2, sv.addAll(List.of(b("1"), b("2"))));
                Assert.assertEquals(2, sv.size());
                Assert.assertTrue(sv.contains(b("1")));
                Assert.assertTrue(sv.estimatedBytes() > 0);

                Assert.assertEquals(1, sv.addAll(List.of(b("alpha"))));
                Assert.assertEquals(ValueEncoding.SET_HT, sv.encoding());
                Assert.assertEquals(3, sv.size());
                Assert.assertTrue(sv.contains(b("alpha")));
                Assert.assertEquals(1, sv.removeAll(List.of(b("2"))));
                Assert.assertFalse(sv.contains(b("2")));
            } finally {
                sv.close();
            }
        }
    }

    @Test
    public void nativeSetStreamsHashtableMembersThroughNativeBytesSlice() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("set-stream-test");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            SetValue sv = new SetValue(allocator);
            try {
                Assert.assertEquals(2, sv.addAll(List.of(b("alpha"), b("beta"))));
                RecordingSink out = new RecordingSink();

                sv.membersInto(out);

                Assert.assertEquals(2, out.values.size());
                Assert.assertTrue(out.values.contains("alpha"));
                Assert.assertTrue(out.values.contains("beta"));
                Assert.assertTrue(out.sawNativeBytesSlice);
            } finally {
                sv.close();
            }
        }
    }

    private static byte[] b(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
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
