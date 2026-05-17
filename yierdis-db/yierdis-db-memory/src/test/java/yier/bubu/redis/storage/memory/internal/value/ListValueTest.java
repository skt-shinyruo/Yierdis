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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ListValueTest {
    @Test
    public void rootCreatedPackedFfmListDoesNotAllocateQuicklistNodeRecords() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-native-node-packed");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             ListRoot root = new ListRoot(runtime, allocator)) {
            ValueHandle handle = root.create();

            root.rpush(handle, List.of(b("a"), b("b"), b("c")));

            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_QUICKLIST_NODE));
        }
    }

    @Test
    public void rootCreatedFfmListAllocatesOneNativeRecordPerQuicklistNodeAfterConversion() {
        int maxBytes = ListValue.quicklistNodeMaxBytesForTesting();
        int elementBytes = elementBytesSoTwoFitThreeDoNot(maxBytes);

        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-native-node-convert");
             NativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 4096);
             ListRoot root = new ListRoot(runtime, allocator)) {
            ValueHandle handle = root.create();

            root.rpush(handle, List.of(new byte[elementBytes], new byte[elementBytes], new byte[elementBytes]));

            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
            Assert.assertEquals(2L, allocator.stats().objectCount(NativeObjectKind.LIST_QUICKLIST_NODE));
        }
    }

    @Test
    public void ffmListValueRequiresNativeAllocatorBackedRoot() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-native-node-direct")) {
            try {
                new ListValue(runtime);
                Assert.fail("expected direct FFM ListValue construction to be unavailable");
            } catch (UnsupportedOperationException expected) {
                Assert.assertTrue(expected.getMessage().contains("ListRoot"));
            }
        }
    }

    @Test
    public void packedListPreservesNullVsEmptyElements() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-test");
             ListRoot root = new ListRoot(runtime)) {
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
             ListRoot root = new ListRoot(runtime, allocator)) {
            ValueHandle handle = root.create();
            List<byte[]> in = new ArrayList<>();
            in.add(new byte[elementBytes]);
            in.add(new byte[elementBytes]);
            in.add(new byte[elementBytes]);

            root.rpush(handle, in);
            Assert.assertEquals(3, root.size(handle));
            Assert.assertEquals(2L, allocator.stats().objectCount(NativeObjectKind.LIST_QUICKLIST_NODE));

            root.lpop(handle, 1);
            Assert.assertEquals(2, root.size(handle));
            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LIST_QUICKLIST_NODE));
        }
    }

    @Test
    public void listRootCloseReleasesQuicklistNodeRecordsAndMetadataFields() {
        int maxBytes = ListValue.quicklistNodeMaxBytesForTesting();
        int elementBytes = elementBytesSoTwoFitThreeDoNot(maxBytes);

        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-test-release");
             NativeAllocator delegate = new YierdisStableNativeAllocator(runtime, 4096);
             FailOnResolveAllocator allocator = new FailOnResolveAllocator(delegate);
             ListRoot root = new ListRoot(runtime, allocator)) {
            ValueHandle handle = root.create();
            root.rpush(handle, List.of(new byte[elementBytes], new byte[elementBytes], new byte[elementBytes]));

            Assert.assertEquals(2L, allocator.stats().objectCount(NativeObjectKind.LIST_QUICKLIST_NODE));
            List<Long> nodeHandles = allocator.allocatedQuicklistHandles();
            Assert.assertEquals(2, nodeHandles.size());

            long firstHandle = nodeHandles.get(0);
            assertNodeMetadata(allocator, firstHandle, handle, 0L, nodeHandles.get(1), 2,
                    encodedListpackEntryBytes(elementBytes) * 2);

            root.release(handle);

            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_QUICKLIST_NODE));
        }
    }

    @Test
    public void removedQuicklistNodeIsClosedWhenMetadataRefreshThrows() {
        int maxBytes = ListValue.quicklistNodeMaxBytesForTesting();
        int elementBytes = elementBytesSoTwoFitThreeDoNot(maxBytes);

        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-test-refresh-failure");
             NativeAllocator delegate = new YierdisStableNativeAllocator(runtime, 4096);
             FailOnResolveAllocator allocator = new FailOnResolveAllocator(delegate);
             ListRoot root = new ListRoot(runtime, allocator)) {
            ValueHandle handle = root.create();
            root.rpush(handle, List.of(new byte[elementBytes], new byte[elementBytes], new byte[elementBytes]));
            Assert.assertEquals(2L, allocator.stats().objectCount(NativeObjectKind.LIST_QUICKLIST_NODE));
            long remainingNodeHandle = allocator.allocatedQuicklistHandles().get(0);

            allocator.failOnQuicklistResolveCall(remainingNodeHandle, 1);
            try {
                root.lpop(handle, 1);
                Assert.fail("expected injected metadata refresh failure");
            } catch (IllegalStateException expected) {
                Assert.assertTrue(expected.getMessage().contains("injected resolve failure"));
            }

            Assert.assertEquals(2, root.size(handle));
            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LIST_QUICKLIST_NODE));
        }
    }

    @Test
    public void addCreatingQuicklistNodeKeepsAppliedSizeAndOwnershipWhenMetadataRefreshThrows() {
        int maxBytes = ListValue.quicklistNodeMaxBytesForTesting();
        int elementBytes = elementBytesSoTwoFitThreeDoNot(maxBytes);

        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-test-add-refresh-failure");
             NativeAllocator delegate = new YierdisStableNativeAllocator(runtime, 4096);
             FailOnResolveAllocator allocator = new FailOnResolveAllocator(delegate);
             ListRoot root = new ListRoot(runtime, allocator)) {
            ValueHandle handle = root.create();
            root.rpush(handle, List.of(new byte[elementBytes], new byte[elementBytes], new byte[elementBytes]));
            root.rpop(handle, 1);
            Assert.assertEquals(2, root.size(handle));
            Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LIST_QUICKLIST_NODE));
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
            Assert.assertEquals(2L, allocator.stats().objectCount(NativeObjectKind.LIST_QUICKLIST_NODE));

            root.release(handle);
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
            Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_QUICKLIST_NODE));
        }
    }

    @Test
    public void failedInitialQuicklistNodeMetadataWriteCleansNativeRecord() {
        int maxBytes = ListValue.quicklistNodeMaxBytesForTesting();
        int elementBytes = elementBytesSoTwoFitThreeDoNot(maxBytes);

        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("list-test-initial-metadata-failure")) {
            NativeAllocator delegate = new YierdisStableNativeAllocator(runtime, 4096);
            try (FailOnResolveAllocator allocator = new FailOnResolveAllocator(delegate);
                 ListRoot root = new ListRoot(runtime, allocator)) {
                ValueHandle handle = root.create();

                allocator.failOnAnyQuicklistResolveCall(1);
                try {
                    root.rpush(handle, List.of(new byte[elementBytes], new byte[elementBytes], new byte[elementBytes]));
                    Assert.fail("expected injected initial metadata write failure");
                } catch (IllegalStateException expected) {
                    Assert.assertTrue(expected.getMessage().contains("injected resolve failure"));
                }

                Assert.assertEquals(0, root.size(handle));
                Assert.assertEquals(1L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
                Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_QUICKLIST_NODE));

                root.release(handle);
                Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_NODE));
                Assert.assertEquals(0L, allocator.stats().objectCount(NativeObjectKind.LIST_QUICKLIST_NODE));
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
            if (kind == NativeObjectKind.LIST_QUICKLIST_NODE) {
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
            return handle.domain() == NativeObjectKind.LIST_QUICKLIST_NODE.domain()
                    && handle.kindCode() == NativeObjectKind.LIST_QUICKLIST_NODE.code();
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
