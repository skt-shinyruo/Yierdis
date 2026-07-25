package yier.bubu.redis.storage.memory.internal.value;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.api.result.ByteValueSink;

import java.nio.charset.StandardCharsets;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NativeListpackTest {
    @Test
    public void storesAllEntriesInOneNativeBlockWithPrimitiveOffsetTopology() throws ReflectiveOperationException {
        try (TestBackend runtime = TestBackend.open("native-listpack-topology");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeListpack lp = new NativeListpack(
                    new NativeByteStore(allocator, NativeObjectKind.HASH_FIELD_BYTES),
                    NativeObjectKind.HASH_FIELD_BYTES
            );
            lp.addLast(bytes("field"), NativeObjectKind.HASH_FIELD_BYTES);
            lp.addLast(bytes("value"), NativeObjectKind.HASH_VALUE_BYTES);
            lp.addLast(null);
            lp.addLast(new byte[0]);

            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LISTPACK_BYTES));
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.HASH_FIELD_BYTES));
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.HASH_VALUE_BYTES));
            List<NativeHandle> handles = new ArrayList<>();
            lp.forEachNativeHandle(handles::add);
            Assert.assertEquals(1, handles.size());

            Field offsetsField = NativeListpack.class.getDeclaredField("entryOffsets");
            offsetsField.setAccessible(true);
            Assert.assertTrue(offsetsField.get(lp) instanceof int[]);
            for (Field field : NativeListpack.class.getDeclaredFields()) {
                if (field.getType().isArray()) {
                    Assert.assertEquals(int[].class, field.getType());
                }
            }

            lp.clear();
        }
    }

    @Test
    public void preservesNullVsEmptyAndSupportsIndexOf() {
        try (TestBackend runtime = TestBackend.open("native-listpack");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeListpack lp = new NativeListpack(new NativeByteStore(allocator, NativeObjectKind.LISTPACK_BYTES),
                    NativeObjectKind.LISTPACK_BYTES);
            lp.addLast(null);
            lp.addLast(new byte[0]);
            lp.addLast(bytes("ab"));

            Assert.assertEquals(3, lp.size());
            Assert.assertEquals(2, lp.rawBytesSize());
            Assert.assertNull(lp.get(0));
            Assert.assertNotNull(lp.get(1));
            Assert.assertEquals(0, lp.get(1).length);
            Assert.assertArrayEquals(bytes("ab"), lp.get(2));
            Assert.assertEquals(0, lp.indexOf(null));
            Assert.assertEquals(1, lp.indexOf(new byte[0]));
            Assert.assertEquals(2, lp.indexOf(bytes("ab")));

            lp.set(0, new byte[0]);
            lp.set(1, null);
            Assert.assertEquals(2, lp.rawBytesSize());
            Assert.assertEquals(0, lp.get(0).length);
            Assert.assertNull(lp.get(1));

            lp.clear();
            Assert.assertEquals(0L, lp.estimatedBytes());
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LISTPACK_BYTES));
        }
    }

    @Test
    public void mutatesOrderAndReleasesRemovedEntries() {
        try (TestBackend runtime = TestBackend.open("native-listpack-mutate");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeListpack lp = new NativeListpack(new NativeByteStore(allocator, NativeObjectKind.LISTPACK_BYTES),
                    NativeObjectKind.LISTPACK_BYTES);
            lp.addLast(bytes("a"));
            lp.addLast(bytes("c"));
            lp.insertAt(1, bytes("b"));
            Assert.assertArrayEquals(bytes("b"), lp.removeAt(1));
            lp.addFirst(bytes("z"));
            lp.set(1, bytes("aa"));

            Assert.assertArrayEquals(bytes("z"), lp.removeFirst());
            Assert.assertArrayEquals(bytes("c"), lp.removeLast());
            Assert.assertArrayEquals(bytes("aa"), lp.get(0));

            lp.clear();
            Assert.assertEquals(0L, lp.estimatedBytes());
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LISTPACK_BYTES));
        }
    }

    @Test
    public void growsStorageAndRetainsCapacityAfterClear() {
        try (TestBackend runtime = TestBackend.open("native-listpack-growth");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeListpack lp = new NativeListpack(
                    new NativeByteStore(allocator, NativeObjectKind.LISTPACK_BYTES),
                    NativeObjectKind.LISTPACK_BYTES
            );
            for (int i = 0; i < 25; i++) {
                lp.addLast(bytes(Integer.toString(i)));
            }

            Assert.assertEquals(25, lp.size());
            Assert.assertArrayEquals(bytes("0"), lp.get(0));
            Assert.assertArrayEquals(bytes("24"), lp.get(24));
            long grownHeapBytes = lp.heapEstimatedBytes();
            Assert.assertEquals(NativeListpack.heapUpperBoundForEntries(25), grownHeapBytes);

            lp.clear();
            Assert.assertEquals(0, lp.size());
            Assert.assertEquals(grownHeapBytes, lp.heapEstimatedBytes());
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LISTPACK_BYTES));
        }
    }

    @Test
    public void preservesOrderWhenEditingAfterGrowth() {
        try (TestBackend runtime = TestBackend.open("native-listpack-growth-edit");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeListpack lp = new NativeListpack(
                    new NativeByteStore(allocator, NativeObjectKind.LISTPACK_BYTES),
                    NativeObjectKind.LISTPACK_BYTES
            );
            for (int i = 0; i < 20; i++) {
                lp.addLast(bytes(Integer.toString(i)));
            }

            lp.insertAt(10, bytes("middle"));
            Assert.assertArrayEquals(bytes("middle"), lp.get(10));
            Assert.assertArrayEquals(bytes("10"), lp.get(11));
            Assert.assertArrayEquals(bytes("19"), lp.get(20));
            Assert.assertArrayEquals(bytes("middle"), lp.removeAt(10));
            Assert.assertEquals(20, lp.size());
            Assert.assertArrayEquals(bytes("10"), lp.get(10));
            Assert.assertArrayEquals(bytes("19"), lp.get(19));

            lp.clear();
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LISTPACK_BYTES));
        }
    }

    @Test
    public void mutatesAcrossVarIntHeaderBoundaries() {
        try (TestBackend runtime = TestBackend.open("native-listpack-varint-boundary");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeListpack lp = new NativeListpack(
                    new NativeByteStore(allocator, NativeObjectKind.LISTPACK_BYTES),
                    NativeObjectKind.LISTPACK_BYTES
            );
            byte[] oneByteHeader = new byte[126];
            byte[] twoByteHeader = new byte[127];
            Arrays.fill(oneByteHeader, (byte) 'a');
            Arrays.fill(twoByteHeader, (byte) 'b');

            lp.addLast(bytes("head"));
            lp.addLast(oneByteHeader);
            lp.addLast(bytes("tail"));
            lp.set(1, twoByteHeader);
            Assert.assertArrayEquals(bytes("head"), lp.get(0));
            Assert.assertArrayEquals(twoByteHeader, lp.get(1));
            Assert.assertArrayEquals(bytes("tail"), lp.get(2));

            lp.set(1, oneByteHeader);
            Assert.assertArrayEquals(oneByteHeader, lp.removeAt(1));
            Assert.assertArrayEquals(bytes("head"), lp.get(0));
            Assert.assertArrayEquals(bytes("tail"), lp.get(1));
            lp.clear();
        }
    }

    @Test
    public void closeExceptReleasesSourceAfterCallerEndsItsPin() {
        try (TestBackend runtime = TestBackend.open("native-listpack-retained");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeByteStore store = new NativeByteStore(allocator, NativeObjectKind.LISTPACK_BYTES);
            NativeListpack source = new NativeListpack(store, NativeObjectKind.LISTPACK_BYTES);
            NativeListpack retained = new NativeListpack(store, NativeObjectKind.LISTPACK_BYTES);
            source.addLast(bytes("keep"));
            source.addLast(bytes("drop"));

            NativeHandle keep = source.entryRefAt(0).handle();
            allocator.pin(keep);
            retained.addBorrowed(source.entryRefAt(0));
            Assert.assertNotEquals(keep.localRaw(), retained.entryRefAt(0).handle().localRaw());

            // 稳定后端不允许 free pinned object；调用方必须先结束自己的 pin。
            allocator.unpin(keep);
            source.closeExcept(retained);
            Assert.assertEquals(0, source.size());
            Assert.assertEquals(1, retained.size());
            Assert.assertArrayEquals(bytes("keep"), retained.get(0));
            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LISTPACK_BYTES));

            retained.clear();
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LISTPACK_BYTES));
            Assert.assertEquals(0L, store.nativeBytes());
        }
    }

    @Test
    public void closeExceptDoesNotAliasSameLocalRawBlocksFromDifferentBackends() {
        try (TestBackend leftRuntime = TestBackend.open("listpack-left-collision");
             TestBackend rightRuntime = TestBackend.open("listpack-right-collision")) {
            StableMemoryBackend leftBackend = leftRuntime.backend();
            StableMemoryBackend rightBackend = rightRuntime.backend();
            NativeListpack source = new NativeListpack(
                    new NativeByteStore(leftBackend, NativeObjectKind.LISTPACK_BYTES),
                    NativeObjectKind.LISTPACK_BYTES
            );
            NativeListpack retained = new NativeListpack(
                    new NativeByteStore(rightBackend, NativeObjectKind.LISTPACK_BYTES),
                    NativeObjectKind.LISTPACK_BYTES
            );
            source.addLast(bytes("source"));
            retained.addLast(bytes("retained"));
            NativeHandle sourceHandle = source.entryRefAt(0).handle();
            NativeHandle retainedHandle = retained.entryRefAt(0).handle();
            try {
                Assert.assertEquals(sourceHandle.localRaw(), retainedHandle.localRaw());
                Assert.assertNotEquals(sourceHandle.allocatorId(), retainedHandle.allocatorId());

                source.closeExcept(retained);

                Assert.assertEquals(0L, leftBackend.stats().objectCount(NativeObjectKind.LISTPACK_BYTES));
                Assert.assertArrayEquals(bytes("retained"), retained.get(0));
            } finally {
                source.close();
                retained.close();
            }
        }
    }

    @Test
    public void cursorStreamsNativeSlicesAndAppendsCopies() {
        try (TestBackend runtime = TestBackend.open("native-listpack-cursor");
             StableMemoryBackend allocator = runtime.backend()) {
            NativeByteStore store = new NativeByteStore(allocator, NativeObjectKind.LISTPACK_BYTES);
            NativeListpack src = new NativeListpack(store, NativeObjectKind.LISTPACK_BYTES);
            src.addLast(bytes("x"));
            src.addLast(null);
            src.addLast(new byte[0]);

            RecordingByteValueSink out = new RecordingByteValueSink();
            NativeListpack dst = new NativeListpack(store, NativeObjectKind.LISTPACK_BYTES);
            NativeListpack.Cursor cursor = src.cursor();
            while (cursor.next()) {
                cursor.writeTo(out);
                cursor.appendTo(dst);
            }

            Assert.assertEquals(Arrays.asList("x", null, ""), out.values);
            Assert.assertTrue(out.sawBytesSlice);
            Assert.assertArrayEquals(bytes("x"), dst.get(0));
            Assert.assertNull(dst.get(1));
            Assert.assertEquals(0, dst.get(2).length);

            src.clear();
            dst.clear();
            Assert.assertEquals(0L, store.nativeBytes());
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LISTPACK_BYTES));
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static final class RecordingByteValueSink implements ByteValueSink {
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
            byte[] out = new byte[slice.length()];
            slice.getBytes(0, out, 0, out.length);
            values.add(new String(out, StandardCharsets.US_ASCII));
        }

        @Override
        public void longAscii(long value) {
            values.add(Long.toString(value));
        }

        @Override
        public void nullValue() {
            value((byte[]) null);
        }
    }
}
