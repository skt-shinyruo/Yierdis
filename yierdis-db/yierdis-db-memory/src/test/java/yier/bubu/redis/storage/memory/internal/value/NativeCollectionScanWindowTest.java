package yier.bubu.redis.storage.memory.internal.value;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.StaleNativeHandleException;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;

public class NativeCollectionScanWindowTest {
    @Test
    public void retainedBytesCoverPinnedNativeAllocationAndEmitReleasesIt() {
        try (TestBackend runtime = TestBackend.open("collection-scan-window");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeByteStore store = new NativeByteStore(allocator, NativeObjectKind.SET_MEMBER_BYTES);
            NativeHandle handle = store.store(bytes("member-value"));
            int allocatedBytes = store.allocatedBytes(handle);
            CollectionScanWindow window;
            try (NativeCollectionScanWindow.Builder builder =
                         NativeCollectionScanWindow.builder(allocator, 1)) {
                builder.addNative(handle, store.length(handle));
                window = builder.build(ScanCursorV2.start());
            }

            Assert.assertTrue(window.retainedMemoryBytes() >= allocatedBytes);
            RecordingSink sink = new RecordingSink(false);

            window.emitTo(sink);

            Assert.assertEquals(List.of("member-value"), sink.values);
            window.close();
            // Heap 后端要求释放前先结束所有显式 pin；窗口 emit 已经完成这次 unpin。
            store.release(handle);
            assertStale(allocator, handle);
        }
    }

    @Test
    public void emitFailureUnpinsEveryElementAndCloseRemainsIdempotent() {
        try (TestBackend runtime = TestBackend.open("collection-scan-window-failure");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeByteStore store = new NativeByteStore(allocator, NativeObjectKind.SET_MEMBER_BYTES);
            NativeHandle first = store.store(bytes("first"));
            NativeHandle second = store.store(bytes("second"));
            CollectionScanWindow window;
            try (NativeCollectionScanWindow.Builder builder =
                         NativeCollectionScanWindow.builder(allocator, 2)) {
                builder.addNative(first, store.length(first));
                builder.addNative(second, store.length(second));
                window = builder.build(ScanCursorV2.start());
            }

            IllegalStateException failure = Assert.assertThrows(
                    IllegalStateException.class,
                    () -> window.emitTo(new RecordingSink(true))
            );

            Assert.assertEquals("injected sink failure", failure.getMessage());
            window.close();
            // emit 失败也必须先收回窗口 pin，之后原 store 才能释放对象。
            store.release(first);
            store.release(second);
            assertStale(allocator, first);
            assertStale(allocator, second);
        }
    }

    @Test
    public void rejectedNativeElementDoesNotLeakItsPin() {
        try (TestBackend runtime = TestBackend.open("collection-scan-window-reject");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeByteStore store = new NativeByteStore(allocator, NativeObjectKind.SET_MEMBER_BYTES);
            NativeHandle handle = store.store(bytes("short"));

            try (NativeCollectionScanWindow.Builder builder =
                         NativeCollectionScanWindow.builder(allocator, 1)) {
                Assert.assertThrows(
                        IndexOutOfBoundsException.class,
                        () -> builder.addNative(handle, store.length(handle) + 1)
                );
            }

            store.release(handle);
            assertStale(allocator, handle);
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static void assertStale(StableMemoryBackend allocator, NativeHandle handle) {
        Assert.assertThrows(
                StaleNativeHandleException.class,
                () -> allocator.resolve(handle, NativeAccessMode.READ_ONLY)
        );
    }

    private static final class RecordingSink implements ByteValueSink {
        private final List<String> values = new ArrayList<>();
        private final boolean fail;

        private RecordingSink(boolean fail) {
            this.fail = fail;
        }

        @Override
        public void value(byte[] data) {
            record(data);
        }

        @Override
        public void value(byte[] data, int off, int len) {
            record(data == null ? null : new String(data, off, len, StandardCharsets.US_ASCII));
        }

        @Override
        public void value(BytesSlice slice) {
            if (fail) {
                throw new IllegalStateException("injected sink failure");
            }
            if (slice == null) {
                values.add(null);
                return;
            }
            byte[] data = new byte[slice.length()];
            slice.getBytes(0, data, 0, data.length);
            record(data);
        }

        @Override
        public void longAscii(long value) {
            record(Long.toString(value));
        }

        @Override
        public void nullValue() {
            value((byte[]) null);
        }

        private void record(byte[] data) {
            record(data == null ? null : new String(data, StandardCharsets.US_ASCII));
        }

        private void record(String value) {
            if (fail) {
                throw new IllegalStateException("injected sink failure");
            }
            values.add(value);
        }
    }
}
