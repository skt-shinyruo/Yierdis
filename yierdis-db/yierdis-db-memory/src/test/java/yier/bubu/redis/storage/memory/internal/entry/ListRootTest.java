package yier.bubu.redis.storage.memory.internal.entry;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.*;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.api.result.ByteValueSink;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ListRootTest {
    @Test
    public void addingNewFfmQuicklistNodeIncrementsQuicklistNodeCountNotRootCount() {
        int elementBytes = 4096;

        try (TestBackend runtime = TestBackend.open("list-root-native-node-add");
             StableMemoryBackend allocator = runtime.backend();
             ListRoot root = new ListRoot(allocator)) {
            ValueHandle handle = root.create();
            root.rpush(handle, List.of(new byte[elementBytes], new byte[elementBytes], new byte[elementBytes]));
            Assert.assertEquals(3L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
            root.rpop(handle, 1);
            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LIST_ROOT));
            Assert.assertEquals(2L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));

            root.rpush(handle, List.of(new byte[elementBytes]));

            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LIST_ROOT));
            Assert.assertEquals(3L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
        }
    }

    @Test
    public void clearReleasesListRootAndQuicklistNodeRecords() {
        int elementBytes = 4096;

        try (TestBackend runtime = TestBackend.open("list-root-clear-release");
             StableMemoryBackend allocator = runtime.backend();
             ListRoot root = new ListRoot(allocator)) {
            ValueHandle handle = root.create();
            root.rpush(handle, List.of(new byte[elementBytes], new byte[elementBytes], new byte[elementBytes]));

            root.clear();

            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_ROOT));
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
        }
    }

    @Test
    public void closeReleasesListRootAndQuicklistNodeRecords() {
        int elementBytes = 4096;

        try (TestBackend runtime = TestBackend.open("list-root-close-release");
             StableMemoryBackend allocator = runtime.backend();
             ListRoot root = new ListRoot(allocator)) {
            ValueHandle handle = root.create();
            root.rpush(handle, List.of(new byte[elementBytes], new byte[elementBytes], new byte[elementBytes]));

            root.close();

            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_ROOT));
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
        }
    }

    @Test
    public void listRootSupportsPushPopAndStreaming() {
        try (TestBackend runtime = TestBackend.open("list-root");
             StableMemoryBackend allocator = runtime.backend();
             ListRoot root = new ListRoot(allocator)) {
            ValueHandle handle = root.create();
            root.rpush(handle, List.of(b("a"), b("b"), b("c")));

            Assert.assertEquals(3, root.size(handle));
            Assert.assertTrue(root.nativeBytes() > 0L);
            RecordingBulkSink out = new RecordingBulkSink();
            root.rangeInto(handle, 0, -1, out);
            Assert.assertTrue(out.sawBytesSlice());
            Assert.assertEquals(List.of("a", "b", "c"), out.values());

            Assert.assertArrayEquals(b("a"), root.lpop(handle, 1).get(0));
            Assert.assertEquals(2, root.size(handle));
        }
    }

    private static byte[] b(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static final class RecordingBulkSink implements ByteValueSink {
        private final List<String> values = new ArrayList<>();
        private boolean sawBytesSlice;

        @Override
        public void value(byte[] data) {
            values.add(data == null ? null : new String(data, StandardCharsets.US_ASCII));
        }

        @Override
        public void value(byte[] data, int off, int len) {
            values.add(data == null ? null : new String(data, off, len, StandardCharsets.US_ASCII));
        }

        @Override
        public void value(BytesSlice slice) {
            if (slice == null) {
                values.add(null);
                return;
            }
            sawBytesSlice = true;
            byte[] raw = new byte[slice.length()];
            slice.getBytes(0, raw, 0, raw.length);
            values.add(new String(raw, StandardCharsets.US_ASCII));
        }

        @Override
        public void longAscii(long value) {
            values.add(Long.toString(value));
        }

        @Override
        public void nullValue() {
            value((byte[]) null);
        }

        private boolean sawBytesSlice() {
            return sawBytesSlice;
        }

        private List<String> values() {
            return values;
        }
    }
}
