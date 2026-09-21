package yier.bubu.redis.storage.memory.internal.value;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.storage.memory.TestBackend;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public class NativeBytesSliceTest {
    @Test
    public void writesNativeBytesAndAllowsDefragAfterWriteCompletes() {
        try (TestBackend runtime = TestBackend.open("native-slice-test");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeHandle handle = allocateString(allocator, "hello");

            NativeBytesSlice slice = new NativeBytesSlice(allocator, handle, 1, 3);
            CollectingSink sink = new CollectingSink();
            slice.writeTo(sink);

            Assert.assertEquals(3, slice.length());
            Assert.assertEquals((byte) 'e', slice.getByte(0));
            Assert.assertEquals("ell", sink.asString());
            allocator.defragCycle(new NativeDefragOptions(1024, 1, Long.MAX_VALUE));
            allocator.free(handle);
        }
    }

    @Test
    public void copiesRangeAndRejectsInvalidBounds() {
        try (TestBackend runtime = TestBackend.open("native-slice-bounds-test");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeHandle handle = allocateString(allocator, "hello");
            try {
                NativeBytesSlice slice = new NativeBytesSlice(allocator, handle, 1, 3);
                byte[] out = new byte[5];

                slice.getBytes(1, out, 2, 2);

                Assert.assertArrayEquals(new byte[] { 0, 0, 'l', 'l', 0 }, out);
                assertThrows(IndexOutOfBoundsException.class, () -> slice.getByte(3));
                assertThrows(IndexOutOfBoundsException.class, () -> slice.getBytes(2, out, 0, 2));
                assertThrows(IndexOutOfBoundsException.class, () -> slice.getBytes(Integer.MAX_VALUE, out, 0, 1));
                assertThrows(IndexOutOfBoundsException.class, () -> slice.getBytes(0, out, Integer.MAX_VALUE, 1));
                assertThrows(IndexOutOfBoundsException.class, () ->
                        new NativeBytesSlice(allocator, handle, 4, 2).writeTo(new CollectingSink()));
            } finally {
                allocator.free(handle);
            }
        }
    }

    @Test
    public void rejectsInvalidConstructorArgumentsAndNullSink() {
        try (TestBackend runtime = TestBackend.open("native-slice-validation-test");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeHandle handle = allocateString(allocator, "hello");
            try {
                assertThrows(NullPointerException.class, () -> new NativeBytesSlice(null, handle, 0, 1));
                assertThrows(NullPointerException.class, () -> new NativeBytesSlice(allocator, null, 0, 1));
                assertThrows(IllegalArgumentException.class, () -> new NativeBytesSlice(allocator, handle, -1, 1));
                assertThrows(IllegalArgumentException.class, () -> new NativeBytesSlice(allocator, handle, 0, -1));
                assertThrows(NullPointerException.class, () ->
                        new NativeBytesSlice(allocator, handle, 0, 1).writeTo(null));
            } finally {
                allocator.free(handle);
            }
        }
    }

    private static NativeHandle allocateString(StableMemoryBackend allocator, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.US_ASCII);
        NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, bytes.length);
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
            view.setBytes(0, bytes, 0, bytes.length);
        }
        return handle;
    }

    private static void assertThrows(Class<? extends Throwable> type, ThrowingRunnable action) {
        try {
            action.run();
            Assert.fail("expected " + type.getName());
        } catch (Throwable thrown) {
            if (!type.isInstance(thrown)) {
                throw new AssertionError("expected " + type.getName() + " but got " + thrown, thrown);
            }
        }
    }

    private interface ThrowingRunnable {
        void run() throws Throwable;
    }

    private static final class CollectingSink implements BytesSink {
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
