package yier.bubu.redis.storage.memory.internal.value;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.memory.TestBackend;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public class NativeByteStoreTest {
    @Test
    public void storesComparesStreamsAndReleasesNativeBytes() {
        try (TestBackend runtime = TestBackend.open("native-byte-store");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeByteStore store = new NativeByteStore(allocator, NativeObjectKind.HASH_VALUE_BYTES);
            NativeHandle handle = store.store(bytes("abc"));

            Assert.assertTrue(store.equalsBytes(handle, bytes("abc")));
            Assert.assertTrue(store.equalsBytes(handle, view(bytes("abc"))));
            Assert.assertFalse(store.equalsBytes(handle, bytes("abd")));
            Assert.assertArrayEquals(bytes("abc"), store.toByteArray(handle));
            Assert.assertEquals(3, store.length(handle));

            BytesSlice slice = store.slice(handle);
            CollectingBytesSink sink = new CollectingBytesSink();
            slice.writeTo(sink);
            Assert.assertEquals("abc", sink.asString());

            store.release(handle);
            Assert.assertEquals(0L, store.nativeBytes());
        }
    }

    @Test
    public void storesEmptyBytesAsNativeHandle() {
        try (TestBackend runtime = TestBackend.open("native-byte-store-empty");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeByteStore store = new NativeByteStore(allocator, NativeObjectKind.HASH_VALUE_BYTES);
            NativeHandle handle = store.store(new byte[0], NativeObjectKind.HASH_FIELD_BYTES);

            Assert.assertEquals(0, store.length(handle));
            Assert.assertArrayEquals(new byte[0], store.toByteArray(handle));
            Assert.assertTrue(store.equalsBytes(handle, new byte[0]));

            store.release(handle);
            Assert.assertEquals(0L, store.nativeBytes());
        }
    }

    private static BytesView view(byte[] bytes) {
        return new BytesView() {
            @Override
            public int length() {
                return bytes.length;
            }

            @Override
            public byte getByte(int index) {
                return bytes[index];
            }
        };
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static final class CollectingBytesSink implements BytesSink {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        @Override
        public void writeBytes(byte[] src, int srcIndex, int len) {
            out.write(src, srcIndex, len);
        }

        String asString() {
            return out.toString(StandardCharsets.US_ASCII);
        }
    }
}
