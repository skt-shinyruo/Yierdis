package yier.bubu.redis.storage.memory.internal.entry;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.OffHeapSlice;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.result.BulkStringSink;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ListRootTest {
    @Test
    public void listRootSupportsPushPopAndStreaming() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-root");
             ListRoot root = new ListRoot(runtime)) {
            ValueHandle handle = root.create();
            root.rpush(handle, List.of(b("a"), b("b"), b("c")));

            Assert.assertEquals(3, root.size(handle));
            Assert.assertTrue(root.nativeBytes() > 0L);
            RecordingBulkSink out = new RecordingBulkSink();
            root.rangeInto(handle, 0, -1, out);
            Assert.assertTrue(out.sawOffHeapSlice());
            Assert.assertEquals(List.of("a", "b", "c"), out.values());

            Assert.assertArrayEquals(b("a"), root.lpop(handle, 1).get(0));
            Assert.assertEquals(2, root.size(handle));
        }
    }

    private static byte[] b(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static final class RecordingBulkSink implements BulkStringSink {
        private final List<String> values = new ArrayList<>();
        private boolean sawOffHeapSlice;

        @Override
        public void bulkString(byte[] data) {
            values.add(data == null ? null : new String(data, StandardCharsets.US_ASCII));
        }

        @Override
        public void bulkString(byte[] data, int off, int len) {
            values.add(data == null ? null : new String(data, off, len, StandardCharsets.US_ASCII));
        }

        @Override
        public void bulkString(BytesSlice slice) {
            if (slice == null) {
                values.add(null);
                return;
            }
            if (slice instanceof OffHeapSlice) {
                sawOffHeapSlice = true;
            }
            byte[] raw = new byte[slice.length()];
            slice.getBytes(0, raw, 0, raw.length);
            values.add(new String(raw, StandardCharsets.US_ASCII));
        }

        @Override
        public void bulkStringLongAscii(long value) {
            values.add(Long.toString(value));
        }

        private boolean sawOffHeapSlice() {
            return sawOffHeapSlice;
        }

        private List<String> values() {
            return values;
        }
    }
}
