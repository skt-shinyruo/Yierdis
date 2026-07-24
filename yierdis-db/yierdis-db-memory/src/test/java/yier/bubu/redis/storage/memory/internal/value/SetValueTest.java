package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.api.result.ByteValueSink;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class SetValueTest {
    @Test
    public void hashtableEncodingDoesNotAllocateValueSlots() throws ReflectiveOperationException {
        try (TestBackend runtime = TestBackend.open("set-without-value-slots");
             StableMemoryBackend allocator = runtime.backend()) {
            SetValue set = new SetValue(allocator);
            try {
                List<byte[]> members = List.of(b("alpha"));
                long heapUpperBound = SetValue.preparedNewHeapUpperBound(members);

                Assert.assertEquals(1, set.addAll(members));
                Assert.assertEquals(ValueEncoding.SET_HT, set.encoding());
                Assert.assertNull(nativeMapValueSlots(set, "members"));
                Assert.assertTrue(set.heapEstimatedBytes() <= heapUpperBound);
                Assert.assertTrue(set.contains(b("alpha")));
            } finally {
                set.close();
            }
        }
    }

    @Test
    public void nativeSetKeepsIntsetMembersAndUpgradesToNativeHashtable() {
        try (TestBackend runtime = TestBackend.open("set-test");
             StableMemoryBackend allocator = runtime.backend()) {
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
        try (TestBackend runtime = TestBackend.open("set-stream-test");
             StableMemoryBackend allocator = runtime.backend()) {
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

    private static Object nativeMapValueSlots(Object owner, String mapFieldName) throws ReflectiveOperationException {
        var mapField = owner.getClass().getDeclaredField(mapFieldName);
        mapField.setAccessible(true);
        Object map = mapField.get(owner);
        var activeField = NativeByteMap.class.getDeclaredField("active");
        activeField.setAccessible(true);
        Object table = activeField.get(map);
        var valueSlotsField = table.getClass().getDeclaredField("valueSlots");
        valueSlotsField.setAccessible(true);
        return valueSlotsField.get(table);
    }

    private static final class RecordingSink implements ByteValueSink {
        private final ArrayList<String> values = new ArrayList<>();
        private boolean sawNativeBytesSlice;

        @Override
        public void value(byte[] data) {
            values.add(data == null ? null : new String(data, StandardCharsets.US_ASCII));
            sawNativeBytesSlice = false;
        }

        @Override
        public void value(byte[] data, int off, int len) {
            values.add(data == null ? null : new String(data, off, len, StandardCharsets.US_ASCII));
            sawNativeBytesSlice = false;
        }

        @Override
        public void value(BytesSlice slice) {
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
        public void longAscii(long value) {
            values.add(Long.toString(value));
            sawNativeBytesSlice = false;
        }

        @Override
        public void nullValue() {
            value((byte[]) null);
        }
    }
}
