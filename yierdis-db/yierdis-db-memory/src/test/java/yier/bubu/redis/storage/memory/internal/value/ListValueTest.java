package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.*;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.memory.internal.entry.ListRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ListValueTest {
    @Test
    public void rootCreatedPackedFfmListDoesNotAllocateQuicklistNodeRecords() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-native-node-packed");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             ListRoot root = new ListRoot(allocator)) {
            ValueHandle handle = root.create();

            root.rpush(handle, List.of(b("a"), b("b"), b("c")));

            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LIST_ROOT));
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
        }
    }

    @Test
    public void rootCreatedPackedListStoresEntriesAsNativeListpackBytesAndStreamsNativeSlices() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-native-packed-bytes");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             ListRoot root = new ListRoot(allocator)) {
            ValueHandle handle = root.create();

            root.rpush(handle, List.of(b("a"), b("b"), b("c")));

            Assert.assertEquals(3L, allocator.stats().objectCount(NativeObjectKind.LISTPACK_BYTES));
            RecordingBulkStringSink out = new RecordingBulkStringSink();
            root.rangeInto(handle, 0, -1, out);
            Assert.assertTrue(out.sawNativeBytesSlice());
            Assert.assertEquals(List.of("a", "b", "c"), out.strings());
        }
    }

    @Test
    public void rootCreatedFfmListAllocatesOneNativeRecordPerQuicklistNodeAfterConversion() {
        int maxBytes = ListValue.quicklistNodeMaxBytesForTesting();
        int elementBytes = elementBytesSoTwoFitThreeDoNot(maxBytes);

        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-native-node-convert");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             ListRoot root = new ListRoot(allocator)) {
            ValueHandle handle = root.create();

            root.rpush(handle, List.of(new byte[elementBytes], new byte[elementBytes], new byte[elementBytes]));

            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LIST_ROOT));
            Assert.assertEquals(2L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
        }
    }

    @Test
    public void listValueExposesOnlyNativeAllocatorBackedConstruction() {
        for (java.lang.reflect.Constructor<?> constructor : ListValue.class.getConstructors()) {
            Assert.assertEquals(2, constructor.getParameterCount());
            Assert.assertEquals(NativeAllocator.class, constructor.getParameterTypes()[0]);
            Assert.assertEquals(NativeHandle.class, constructor.getParameterTypes()[1]);
        }
    }

    @Test
    public void packedListPreservesNullVsEmptyElements() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-test");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             ListRoot root = new ListRoot(allocator)) {
            ValueHandle handle = root.create();
            root.rpush(handle, Arrays.asList(null, new byte[0], "a".getBytes(StandardCharsets.US_ASCII)));

            Assert.assertEquals(3, root.size(handle));

            List<byte[]> all = readRange(root, handle, 0, -1);
            Assert.assertEquals(3, all.size());
            Assert.assertNull(all.get(0));
            Assert.assertNotNull(all.get(1));
            Assert.assertEquals(0, all.get(1).length);
            Assert.assertArrayEquals("a".getBytes(StandardCharsets.US_ASCII), all.get(2));

            List<byte[]> popped = root.lpop(handle, 1);
            Assert.assertEquals(1, popped.size());
            Assert.assertNull(popped.get(0));
            Assert.assertEquals(2, root.size(handle));
        }
    }

    @Test
    public void quicklistSplitsByBytesAndMerges() {
        int maxBytes = ListValue.quicklistNodeMaxBytesForTesting();
        int elementBytes = elementBytesSoTwoFitThreeDoNot(maxBytes);

        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-test");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             ListRoot root = new ListRoot(allocator)) {
            ValueHandle handle = root.create();
            List<byte[]> in = new ArrayList<>();
            in.add(new byte[elementBytes]);
            in.add(new byte[elementBytes]);
            in.add(new byte[elementBytes]);

            root.rpush(handle, in);
            Assert.assertEquals(3, root.size(handle));
            Assert.assertEquals(2L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));

            root.lpop(handle, 1);
            Assert.assertEquals(2, root.size(handle));
            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
        }
    }

    @Test
    public void poppingElementThatEmptiesFfmQuicklistNodeFreesExactlyThatNodeRecord() {
        int elementBytes = ListValue.quicklistNodeMaxBytesForTesting();
        byte[] a = filledValue('a', elementBytes);
        byte[] b = filledValue('b', elementBytes);
        byte[] c = filledValue('c', elementBytes);

        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-test-pop-free-node");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             ListRoot root = new ListRoot(allocator)) {
            ValueHandle handle = root.create();
            root.rpush(handle, List.of(a, b, c));
            List<Long> nodeHandles = quicklistNodeHandles(root, handle);
            Assert.assertEquals(3, nodeHandles.size());

            Assert.assertArrayEquals(a, root.lpop(handle, 1).get(0));

            Assert.assertEquals(2L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
            assertStale(allocator, nodeHandles.get(0));
            assertLive(allocator, nodeHandles.get(1));
            assertLive(allocator, nodeHandles.get(2));
            List<byte[]> remaining = readRange(root, handle, 0, -1);
            Assert.assertArrayEquals(b, remaining.get(0));
            Assert.assertArrayEquals(c, remaining.get(1));
        }
    }

    @Test
    public void firstNodeMergeFreesDiscardedQuicklistNodeAndPreservesOrder() {
        int maxBytes = ListValue.quicklistNodeMaxBytesForTesting();
        int elementBytes = elementBytesSoTwoFitThreeDoNot(maxBytes);
        byte[] a = filledValue('a', elementBytes);
        byte[] b = filledValue('b', elementBytes);
        byte[] c = filledValue('c', elementBytes);

        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-test-first-merge-free");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             ListRoot root = new ListRoot(allocator)) {
            ValueHandle handle = root.create();
            root.rpush(handle, List.of(a, b, c));
            List<Long> nodeHandles = quicklistNodeHandles(root, handle);
            Assert.assertEquals(2, nodeHandles.size());

            Assert.assertArrayEquals(a, root.lpop(handle, 1).get(0));

            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
            assertLive(allocator, nodeHandles.get(0));
            assertStale(allocator, nodeHandles.get(1));
            List<byte[]> remaining = readRange(root, handle, 0, -1);
            Assert.assertEquals(2, remaining.size());
            Assert.assertArrayEquals(b, remaining.get(0));
            Assert.assertArrayEquals(c, remaining.get(1));
        }
    }

    @Test
    public void lastNodeMergeFreesDiscardedQuicklistNodeAndPreservesOrder() {
        int maxBytes = ListValue.quicklistNodeMaxBytesForTesting();
        int elementBytes = elementBytesSoTwoFitThreeDoNot(maxBytes);
        byte[] a = filledValue('a', elementBytes);
        byte[] b = filledValue('b', elementBytes);
        byte[] c = filledValue('c', elementBytes);

        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-test-last-merge-free");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             ListRoot root = new ListRoot(allocator)) {
            ValueHandle handle = root.create();
            root.lpush(handle, List.of(c, b, a));
            List<Long> nodeHandles = quicklistNodeHandles(root, handle);
            Assert.assertEquals(2, nodeHandles.size());

            Assert.assertArrayEquals(c, root.rpop(handle, 1).get(0));

            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
            assertLive(allocator, nodeHandles.get(0));
            assertStale(allocator, nodeHandles.get(1));
            List<byte[]> remaining = readRange(root, handle, 0, -1);
            Assert.assertEquals(2, remaining.size());
            Assert.assertArrayEquals(a, remaining.get(0));
            Assert.assertArrayEquals(b, remaining.get(1));
        }
    }

    @Test
    public void removedQuicklistNodeHandleAndAdapterStayStaleAfterFurtherAllocations() throws Exception {
        int elementBytes = ListValue.quicklistNodeMaxBytesForTesting();
        byte[] a = filledValue('a', elementBytes);
        byte[] b = filledValue('b', elementBytes);
        byte[] c = filledValue('c', elementBytes);
        byte[] d = filledValue('d', elementBytes);

        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-test-stale-node");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 7);
             ListRoot root = new ListRoot(allocator)) {
            ValueHandle handle = root.create();
            root.rpush(handle, List.of(a, b, c));
            Object staleAdapter = firstQuicklistNodeAdapter(root, handle);
            long staleHandle = rawQuicklistNodeHandle(staleAdapter);
            NativeHandle staleNativeHandle = NativeHandle.fromRaw(staleHandle);

            root.lpop(handle, 1);
            assertStale(allocator, staleHandle);

            root.rpush(handle, List.of(d));

            for (long liveHandle : quicklistNodeHandles(root, handle)) {
                Assert.assertNotEquals(staleNativeHandle.raw(), liveHandle);
                assertLive(allocator, liveHandle);
            }
            assertStale(allocator, staleHandle);
            assertStaleAdapterFailsThroughAllocator(staleAdapter);
        }
    }

    @Test
    public void corruptedQuicklistNodeAdapterHandleFailsNativeValidationBeforePayloadRead() throws Exception {
        int elementBytes = ListValue.quicklistNodeMaxBytesForTesting();
        byte[] a = filledValue('a', elementBytes);
        byte[] b = filledValue('b', elementBytes);
        byte[] c = filledValue('c', elementBytes);

        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-test-corrupt-node-handle");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             ListRoot root = new ListRoot(allocator)) {
            ValueHandle handle = root.create();
            root.rpush(handle, List.of(a, b, c));
            Object staleAdapter = firstQuicklistNodeAdapter(root, handle);
            long staleHandle = rawQuicklistNodeHandle(staleAdapter);

            root.lpop(handle, 1);
            assertStale(allocator, staleHandle);

            NativeHandle wrongKindHandle = NativeHandle.of(
                    NativeObjectKind.HASH_ROOT.domain(),
                    NativeObjectKind.HASH_ROOT,
                    NativeHandle.fromRaw(staleHandle).slotId(),
                    NativeHandle.fromRaw(staleHandle).generation(),
                    NativeHandle.fromRaw(staleHandle).flags()
            );
            setQuicklistNodeHandle(staleAdapter, wrongKindHandle);
            assertCorruptedAdapterFailsNativeValidation(staleAdapter);

            NativeHandle wrongDomainHandle = NativeHandle.of(
                    NativeObjectKind.ENTRY_RECORD.domain(),
                    NativeObjectKind.ENTRY_RECORD,
                    NativeHandle.fromRaw(staleHandle).slotId(),
                    NativeHandle.fromRaw(staleHandle).generation(),
                    NativeHandle.fromRaw(staleHandle).flags()
            );
            setQuicklistNodeHandle(staleAdapter, wrongDomainHandle);
            assertCorruptedAdapterFailsNativeValidation(staleAdapter);
        }
    }

    @Test
    public void defragMovesQuicklistNodeRecordWithoutChangingHandleOrTreatingPayloadAsAllocatorObject() {
        int maxBytes = ListValue.quicklistNodeMaxBytesForTesting();
        int elementBytes = elementBytesSoTwoFitThreeDoNot(maxBytes);
        byte[] a = filledValue('a', elementBytes);
        byte[] b = filledValue('b', elementBytes);
        byte[] c = filledValue('c', elementBytes);
        byte[] d = filledValue('d', elementBytes);

        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-test-defrag-node");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             ListRoot root = new ListRoot(allocator)) {
            ValueHandle handle = root.create();
            root.rpush(handle, List.of(a, b, c));
            long nodeHandleRaw = quicklistNodeHandles(root, handle).get(0);

            NativeDefragResult result = allocator.defragOne(NativeHandle.fromRaw(nodeHandleRaw), 48);

            Assert.assertTrue(result.moved());
            Assert.assertEquals(48L, result.movedBytes());
            assertLive(allocator, nodeHandleRaw);
            Assert.assertEquals(2L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.STRING_BYTES));

            root.rpush(handle, List.of(d));

            List<byte[]> values = readRange(root, handle, 0, -1);
            Assert.assertEquals(4, values.size());
            Assert.assertArrayEquals(a, values.get(0));
            Assert.assertArrayEquals(b, values.get(1));
            Assert.assertArrayEquals(c, values.get(2));
            Assert.assertArrayEquals(d, values.get(3));
            Assert.assertEquals(2L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
        }
    }

    @Test
    public void listRootCloseReleasesQuicklistNodeRecordsAndMetadataFields() {
        int maxBytes = ListValue.quicklistNodeMaxBytesForTesting();
        int elementBytes = elementBytesSoTwoFitThreeDoNot(maxBytes);

        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-test-release");
             NativeAllocator delegate = new YierdisStableNativeAllocator(runtime, 4096);
             FailOnResolveAllocator allocator = new FailOnResolveAllocator(delegate);
             ListRoot root = new ListRoot(allocator)) {
            ValueHandle handle = root.create();
            root.rpush(handle, List.of(new byte[elementBytes], new byte[elementBytes], new byte[elementBytes]));

            Assert.assertEquals(2L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
            List<Long> nodeHandles = allocator.allocatedQuicklistHandles();
            Assert.assertEquals(2, nodeHandles.size());

            long firstHandle = nodeHandles.get(0);
            assertNodeMetadata(allocator, firstHandle, handle, 0L, nodeHandles.get(1), 2,
                    encodedListpackEntryBytes(elementBytes) * 2);

            root.release(handle);

            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_ROOT));
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
        }
    }

    @Test
    public void removedQuicklistNodeIsClosedWhenMetadataRefreshThrows() {
        int maxBytes = ListValue.quicklistNodeMaxBytesForTesting();
        int elementBytes = elementBytesSoTwoFitThreeDoNot(maxBytes);

        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-test-refresh-failure");
             NativeAllocator delegate = new YierdisStableNativeAllocator(runtime, 4096);
             FailOnResolveAllocator allocator = new FailOnResolveAllocator(delegate);
             ListRoot root = new ListRoot(allocator)) {
            ValueHandle handle = root.create();
            root.rpush(handle, List.of(new byte[elementBytes], new byte[elementBytes], new byte[elementBytes]));
            Assert.assertEquals(2L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
            long remainingNodeHandle = allocator.allocatedQuicklistHandles().get(0);

            allocator.failOnQuicklistResolveCall(remainingNodeHandle, 1);
            try {
                root.lpop(handle, 1);
                Assert.fail("expected injected metadata refresh failure");
            } catch (IllegalStateException expected) {
                Assert.assertTrue(expected.getMessage().contains("injected resolve failure"));
            }

            Assert.assertEquals(2, root.size(handle));
            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
        }
    }

    @Test
    public void addCreatingQuicklistNodeKeepsAppliedSizeAndOwnershipWhenMetadataRefreshThrows() {
        int maxBytes = ListValue.quicklistNodeMaxBytesForTesting();
        int elementBytes = elementBytesSoTwoFitThreeDoNot(maxBytes);

        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-test-add-refresh-failure");
             NativeAllocator delegate = new YierdisStableNativeAllocator(runtime, 4096);
             FailOnResolveAllocator allocator = new FailOnResolveAllocator(delegate);
             ListRoot root = new ListRoot(allocator)) {
            ValueHandle handle = root.create();
            root.rpush(handle, List.of(new byte[elementBytes], new byte[elementBytes], new byte[elementBytes]));
            root.rpop(handle, 1);
            Assert.assertEquals(2, root.size(handle));
            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
            long existingNodeHandle = allocator.allocatedQuicklistHandles().get(0);

            allocator.failOnQuicklistResolveCall(existingNodeHandle, 1);
            try {
                root.rpush(handle, List.of(new byte[elementBytes]));
                Assert.fail("expected injected metadata refresh failure");
            } catch (IllegalStateException expected) {
                Assert.assertTrue(expected.getMessage().contains("injected resolve failure"));
            }

            Assert.assertEquals(3, root.size(handle));
            Assert.assertEquals(3, readRange(root, handle, 0, -1).size());
            Assert.assertEquals(2L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));

            root.release(handle);
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_ROOT));
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
        }
    }

    @Test
    public void failedInitialQuicklistNodeMetadataWriteCleansNativeRecord() {
        int maxBytes = ListValue.quicklistNodeMaxBytesForTesting();
        int elementBytes = elementBytesSoTwoFitThreeDoNot(maxBytes);

        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-test-initial-metadata-failure")) {
            NativeAllocator delegate = new YierdisStableNativeAllocator(runtime, 4096);
            try (FailOnResolveAllocator allocator = new FailOnResolveAllocator(delegate);
                 ListRoot root = new ListRoot(allocator)) {
                ValueHandle handle = root.create();

                allocator.failOnAnyQuicklistResolveCall(1);
                try {
                    root.rpush(handle, List.of(new byte[elementBytes], new byte[elementBytes], new byte[elementBytes]));
                    Assert.fail("expected injected initial metadata write failure");
                } catch (IllegalStateException expected) {
                    Assert.assertTrue(expected.getMessage().contains("injected resolve failure"));
                }

                Assert.assertEquals(0, root.size(handle));
                Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LIST_ROOT));
                Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));

                root.release(handle);
                Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_ROOT));
                Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
            }
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    private static int elementBytesSoTwoFitThreeDoNot(int maxNodeBytes) {
        // Finds a stable "large element" size such that:
        // - 2 encoded entries fit within a node
        // - 3 encoded entries do not fit within a node
        //
        // This keeps the test robust to small encoding changes (e.g., varint header sizes).
        for (int candidate = Math.max(65, maxNodeBytes); candidate >= 65; candidate--) {
            int entryBytes = encodedListpackEntryBytes(candidate);
            if ((long) entryBytes * 2 <= maxNodeBytes && (long) entryBytes * 3 > maxNodeBytes) {
                return candidate;
            }
        }
        throw new AssertionError("unable to find element size for node bytes=" + maxNodeBytes);
    }

    private static byte[] b(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] filledValue(char value, int length) {
        byte[] out = new byte[length];
        Arrays.fill(out, (byte) value);
        return out;
    }

    private static List<byte[]> readRange(ListRoot root, ValueHandle handle, int start, int stop) {
        List<byte[]> out = new ArrayList<>();
        root.rangeInto(handle, start, stop, new BulkStringSink() {
            @Override
            public void bulkString(byte[] data) {
                out.add(data);
            }

            @Override
            public void bulkString(byte[] data, int off, int len) {
                out.add(data == null ? null : Arrays.copyOfRange(data, off, off + len));
            }

            @Override
            public void bulkString(yier.bubu.redis.bytes.BytesSlice slice) {
                if (slice == null) {
                    out.add(null);
                    return;
                }
                byte[] data = new byte[slice.length()];
                slice.getBytes(0, data, 0, data.length);
                out.add(data);
            }

            @Override
            public void bulkStringLongAscii(long value) {
                out.add(Long.toString(value).getBytes(StandardCharsets.US_ASCII));
            }
        });
        return out;
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
        public void bulkString(yier.bubu.redis.bytes.BytesSlice slice) {
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

    private static List<Long> quicklistNodeHandles(ListRoot root, ValueHandle handle) {
        ArrayDeque<?> quicklist = quicklist(root, handle);
        List<Long> out = new ArrayList<>();
        for (Object node : quicklist) {
            out.add(rawQuicklistNodeHandle(node));
        }
        return out;
    }

    private static Object firstQuicklistNodeAdapter(ListRoot root, ValueHandle handle) {
        return quicklist(root, handle).peekFirst();
    }

    @SuppressWarnings("unchecked")
    private static ArrayDeque<?> quicklist(ListRoot root, ValueHandle handle) {
        try {
            Field listsField = ListRoot.class.getDeclaredField("lists");
            listsField.setAccessible(true);
            Object lists = listsField.get(root);
            Field adaptersField = lists.getClass().getDeclaredField("adapters");
            adaptersField.setAccessible(true);
            Map<Long, ListValue> adapters = (Map<Long, ListValue>) adaptersField.get(lists);
            ListValue value = adapters.get(handle.raw());
            Field quicklistField = ListValue.class.getDeclaredField("quicklist");
            quicklistField.setAccessible(true);
            return (ArrayDeque<?>) quicklistField.get(value);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static long rawQuicklistNodeHandle(Object node) {
        try {
            Method rawHandle = node.getClass().getDeclaredMethod("rawHandle");
            rawHandle.setAccessible(true);
            return (Long) rawHandle.invoke(node);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static void setQuicklistNodeHandle(Object node, NativeHandle handle) {
        try {
            Field nodeHandle = node.getClass().getDeclaredField("nodeHandle");
            nodeHandle.setAccessible(true);
            nodeHandle.set(node, handle);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static void assertStaleAdapterFailsThroughAllocator(Object staleAdapter) throws Exception {
        Method removeFirst = staleAdapter.getClass().getDeclaredMethod("removeFirst");
        removeFirst.setAccessible(true);
        try {
            removeFirst.invoke(staleAdapter);
            Assert.fail("expected stale native handle");
        } catch (InvocationTargetException e) {
            Assert.assertTrue("expected stale native handle but got " + e.getCause(),
                    e.getCause() instanceof StaleNativeHandleException);
        }
    }

    private static void assertCorruptedAdapterFailsNativeValidation(Object staleAdapter) throws Exception {
        Method removeFirst = staleAdapter.getClass().getDeclaredMethod("removeFirst");
        removeFirst.setAccessible(true);
        try {
            removeFirst.invoke(staleAdapter);
            Assert.fail("expected native handle validation failure");
        } catch (InvocationTargetException e) {
            Assert.assertTrue("expected native validation failure but got " + e.getCause(),
                    e.getCause() instanceof NativeMemoryException);
            Assert.assertTrue(e.getCause().getMessage().contains("LIST_NODE"));
        }
    }

    private static void assertLive(NativeAllocator allocator, long handleRaw) {
        allocator.resolve(NativeHandle.fromRaw(handleRaw), NativeAccessMode.READ_ONLY).close();
    }

    private static void assertStale(NativeAllocator allocator, long handleRaw) {
        try {
            allocator.resolve(NativeHandle.fromRaw(handleRaw), NativeAccessMode.READ_ONLY).close();
            Assert.fail("expected stale native handle");
        } catch (StaleNativeHandleException expected) {
            // expected
        }
    }

    private static int encodedListpackEntryBytes(int rawLen) {
        int headerValue = rawLen + 1;
        return varIntSize(headerValue) + rawLen;
    }

    private static int varIntSize(int value) {
        int bytes = 1;
        int v = value;
        while ((v & ~0x7F) != 0) {
            v >>>= 7;
            bytes++;
        }
        return bytes;
    }

    private static void assertNodeMetadata(
            NativeAllocator allocator,
            long nodeHandleRaw,
            ValueHandle rootHandle,
            long expectedPrev,
            long expectedNext,
            int expectedEntryCount,
            int expectedEncodedBytes
    ) {
        try (yier.bubu.redis.memory.api.NativeObjectView view = allocator.resolve(
                yier.bubu.redis.memory.api.NativeHandle.fromRaw(nodeHandleRaw),
                yier.bubu.redis.memory.api.NativeAccessMode.READ_ONLY)) {
            Assert.assertEquals(rootHandle.raw(), getLong(view, 0));
            Assert.assertEquals(expectedPrev, getLong(view, 8));
            Assert.assertEquals(expectedNext, getLong(view, 16));
            Assert.assertEquals(0L, getLong(view, 24));
            Assert.assertEquals(expectedEntryCount, getInt(view, 32));
            Assert.assertEquals(expectedEncodedBytes, getInt(view, 36));
            Assert.assertEquals(0, getInt(view, 40));
            Assert.assertEquals(0, getInt(view, 44));
        }
    }

    private static long getLong(yier.bubu.redis.memory.api.NativeObjectView view, int offset) {
        return ((long) view.getByte(offset) & 0xff)
                | (((long) view.getByte(offset + 1) & 0xff) << 8)
                | (((long) view.getByte(offset + 2) & 0xff) << 16)
                | (((long) view.getByte(offset + 3) & 0xff) << 24)
                | (((long) view.getByte(offset + 4) & 0xff) << 32)
                | (((long) view.getByte(offset + 5) & 0xff) << 40)
                | (((long) view.getByte(offset + 6) & 0xff) << 48)
                | (((long) view.getByte(offset + 7) & 0xff) << 56);
    }

    private static int getInt(yier.bubu.redis.memory.api.NativeObjectView view, int offset) {
        return (view.getByte(offset) & 0xff)
                | ((view.getByte(offset + 1) & 0xff) << 8)
                | ((view.getByte(offset + 2) & 0xff) << 16)
                | ((view.getByte(offset + 3) & 0xff) << 24);
    }

    private static final class FailOnResolveAllocator implements NativeAllocator {
        private final NativeAllocator delegate;
        private final List<Long> allocatedQuicklistHandles = new ArrayList<>();
        private long failHandleRaw;
        private int failOnTargetResolveCall;
        private int targetResolveCalls;
        private int failOnAnyQuicklistResolveCall;
        private int anyQuicklistResolveCalls;

        private FailOnResolveAllocator(NativeAllocator delegate) {
            this.delegate = delegate;
        }

        private void failOnQuicklistResolveCall(long handleRaw, int call) {
            this.failHandleRaw = handleRaw;
            this.failOnTargetResolveCall = call;
            this.targetResolveCalls = 0;
        }

        private void failOnAnyQuicklistResolveCall(int call) {
            this.failOnAnyQuicklistResolveCall = call;
            this.anyQuicklistResolveCalls = 0;
        }

        private List<Long> allocatedQuicklistHandles() {
            return allocatedQuicklistHandles;
        }

        @Override
        public NativeHandle allocate(NativeObjectKind kind, int size) {
            NativeHandle handle = delegate.allocate(kind, size);
            if (kind == NativeObjectKind.LIST_NODE) {
                allocatedQuicklistHandles.add(handle.raw());
            }
            return handle;
        }

        @Override
        public NativeHandle realloc(NativeHandle handle, int newSize, NativeReallocPolicy policy) {
            return delegate.realloc(handle, newSize, policy);
        }

        @Override
        public void free(NativeHandle handle) {
            delegate.free(handle);
            allocatedQuicklistHandles.remove(handle.raw());
        }

        @Override
        public void pin(NativeHandle handle) {
            delegate.pin(handle);
        }

        @Override
        public void unpin(NativeHandle handle) {
            delegate.unpin(handle);
        }

        @Override
        public NativeEpochScope beginEpoch(NativeEpochKind kind) {
            return delegate.beginEpoch(kind);
        }

        @Override
        public NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode) {
            if (failHandleRaw == handle.raw()
                    && mode == NativeAccessMode.READ_WRITE
                    && ++targetResolveCalls == failOnTargetResolveCall) {
                throw new IllegalStateException("injected resolve failure");
            }
            if (isQuicklistNode(handle)
                    && mode == NativeAccessMode.READ_WRITE
                    && ++anyQuicklistResolveCalls == failOnAnyQuicklistResolveCall) {
                throw new IllegalStateException("injected resolve failure");
            }
            return delegate.resolve(handle, mode);
        }

        private static boolean isQuicklistNode(NativeHandle handle) {
            return handle.domain() == NativeObjectKind.LIST_NODE.domain()
                    && handle.kindCode() == NativeObjectKind.LIST_NODE.code();
        }

        @Override
        public NativeDefragResult defragOne(NativeHandle handle, long maxMoveBytes) {
            return delegate.defragOne(handle, maxMoveBytes);
        }

        @Override
        public NativeDefragReport defragCycle(NativeDefragOptions options) {
            return delegate.defragCycle(options);
        }

        @Override
        public NativeAllocatorStats stats() {
            return delegate.stats();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
