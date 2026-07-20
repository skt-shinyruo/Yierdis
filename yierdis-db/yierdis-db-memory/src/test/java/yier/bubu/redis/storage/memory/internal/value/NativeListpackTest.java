package yier.bubu.redis.storage.memory.internal.value;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.api.result.BulkStringSink;

import java.nio.charset.StandardCharsets;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NativeListpackTest {
    @Test
    public void storesAllEntriesInOneNativeBlockWithPrimitiveOffsetTopology() throws ReflectiveOperationException {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-listpack-topology");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
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
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-listpack");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
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
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-listpack-mutate");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
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
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-listpack-growth");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
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
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-listpack-growth-edit");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
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
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-listpack-varint-boundary");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
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
    public void closeExceptRetainsPinnedOwnershipUntilTheRetainedOwnerReleasesIt() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-listpack-retained");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            NativeByteStore store = new NativeByteStore(allocator, NativeObjectKind.LISTPACK_BYTES);
            NativeListpack source = new NativeListpack(store, NativeObjectKind.LISTPACK_BYTES);
            NativeListpack retained = new NativeListpack(store, NativeObjectKind.LISTPACK_BYTES);
            source.addLast(bytes("keep"));
            source.addLast(bytes("drop"));

            NativeHandle keep = source.entryRefAt(0).handle();
            allocator.pin(keep);
            retained.addBorrowed(source.entryRefAt(0));
            Assert.assertNotEquals(keep.raw(), retained.entryRefAt(0).handle().raw());

            source.closeExcept(retained);
            Assert.assertEquals(0, source.size());
            Assert.assertEquals(1, retained.size());
            Assert.assertArrayEquals(bytes("keep"), retained.get(0));
            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LISTPACK_BYTES));

            retained.clear();
            allocator.unpin(keep);
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LISTPACK_BYTES));
            Assert.assertEquals(0L, store.nativeBytes());
        }
    }

    @Test
    public void cursorStreamsNativeSlicesAndAppendsCopies() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-listpack-cursor");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096)) {
            NativeByteStore store = new NativeByteStore(allocator, NativeObjectKind.LISTPACK_BYTES);
            NativeListpack src = new NativeListpack(store, NativeObjectKind.LISTPACK_BYTES);
            src.addLast(bytes("x"));
            src.addLast(null);
            src.addLast(new byte[0]);

            RecordingBulkStringSink out = new RecordingBulkStringSink();
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

    private static final class RecordingBulkStringSink implements BulkStringSink {
        private final List<String> values = new ArrayList<>();
        private boolean sawBytesSlice;

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
            sawBytesSlice = true;
            byte[] out = new byte[slice.length()];
            slice.getBytes(0, out, 0, out.length);
            values.add(new String(out, StandardCharsets.US_ASCII));
        }

        @Override
        public void bulkStringLongAscii(long value) {
            values.add(Long.toString(value));
        }
    }
}
