package yier.bubu.redis.storage.memory.internal.value;

import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.memory.internal.entry.ListRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class ListValueTest {
    private static final int QUICKLIST_NODE_RECORD_BYTES = 80;
    private static final int QUICKLIST_NODE_OWNER_ROOT_OFFSET = 0;
    private static final int QUICKLIST_NODE_PREV_OFFSET = 16;
    private static final int QUICKLIST_NODE_NEXT_OFFSET = 32;

    @Test
    public void preparedFactoriesExposeExplicitOperationVariants() {
        try (TestBackend runtime = TestBackend.open("list-value-prepared-operations");
             ListRoot root = new ListRoot(runtime.backend())) {
            ValueHandle packed = root.build(List.of(b("value")));
            ValueHandle quicklist = root.build(List.of(filled('a', 5_000), filled('b', 5_000), b("tail")));
            try {
                try (ListValue.PreparedMutation prepared = root.preparePush(packed, List.of(), true)) {
                    Assert.assertEquals(ListValue.PreparedMutation.Operation.UNCHANGED, prepared.operation());
                }
                try (ListValue.PreparedMutation prepared = root.preparePush(packed, List.of(b("next")), false)) {
                    Assert.assertEquals(
                            ListValue.PreparedMutation.Operation.PACKED_REPLACEMENT,
                            prepared.operation()
                    );
                }
                try (ListValue.PreparedMutation prepared = root.preparePush(
                        packed,
                        List.of(filled('c', 5_000), filled('d', 5_000)),
                        false
                )) {
                    Assert.assertEquals(
                            ListValue.PreparedMutation.Operation.PACKED_TO_QUICKLIST,
                            prepared.operation()
                    );
                }
                try (ListValue.PreparedMutation prepared = root.preparePush(quicklist, List.of(b("next")), true)) {
                    Assert.assertEquals(ListValue.PreparedMutation.Operation.QUICKLIST_PUSH, prepared.operation());
                }
                try (ListValue.PreparedMutation prepared = root.preparePop(quicklist, 1, false)) {
                    Assert.assertEquals(ListValue.PreparedMutation.Operation.QUICKLIST_POP, prepared.operation());
                }
            } finally {
                root.release(packed);
                root.release(quicklist);
            }
        }
    }

    @Test
    public void preparedPackedPushCanBeAbortedAndCommittedWithoutChangingOrder() {
        try (TestBackend runtime = TestBackend.open("list-value-prepared-packed");
             StableMemoryBackend backend = runtime.backend();
             ListRoot root = new ListRoot(backend)) {
            ValueHandle handle = root.build(List.of(b("middle")));
            long liveBefore = backend.stats().liveObjects();

            try (ListValue.PreparedMutation prepared = root.preparePush(
                    handle,
                    List.of(b("left-1"), b("left-2")),
                    true
            )) {
                Assert.assertEquals(3, prepared.size());
                Assert.assertEquals(ValueEncoding.LIST_PACKED, prepared.encoding());
            }

            assertValues(root, handle, b("middle"));
            Assert.assertEquals(liveBefore, backend.stats().liveObjects());

            try (ListValue.PreparedMutation prepared = root.preparePush(
                    handle,
                    List.of(b("left-1"), b("left-2")),
                    true
            )) {
                prepared.commit();
                prepared.releaseSuperseded();
            }

            assertValues(root, handle, b("left-2"), b("left-1"), b("middle"));
            root.release(handle);
            Assert.assertEquals(0L, backend.stats().liveObjects());
        }
    }

    @Test
    public void preparedPushPromotesPackedListAndPreservesLeftPushOrder() {
        try (TestBackend runtime = TestBackend.open("list-value-prepared-promotion");
             StableMemoryBackend backend = runtime.backend();
             ListRoot root = new ListRoot(backend)) {
            ValueHandle handle = root.build(List.of(b("tail")));
            byte[] first = filled('a', 5_000);
            byte[] second = filled('b', 5_000);

            try (ListValue.PreparedMutation prepared = root.preparePush(
                    handle,
                    List.of(first, second),
                    true
            )) {
                Assert.assertEquals(ValueEncoding.LIST_QUICKLIST, prepared.encoding());
                prepared.commit();
                prepared.releaseSuperseded();
            }

            Assert.assertEquals(ValueEncoding.LIST_QUICKLIST, root.encoding(handle));
            assertValues(root, handle, second, first, b("tail"));
            root.release(handle);
            Assert.assertEquals(0L, backend.stats().liveObjects());
        }
    }

    @Test
    public void preparedQuicklistPopHandlesPartialAndWholeNodesAtBothEdges() {
        try (TestBackend runtime = TestBackend.open("list-value-prepared-quicklist-pop");
             StableMemoryBackend backend = runtime.backend();
             ListRoot root = new ListRoot(backend)) {
            byte[] first = filled('a', 5_000);
            byte[] second = filled('b', 5_000);
            ValueHandle handle = root.build(List.of(first, second, b("tail")));
            Assert.assertEquals(ValueEncoding.LIST_QUICKLIST, root.encoding(handle));
            try (ListValue.PreparedMutation prepared = root.preparePop(handle, 1, false)) {
                prepared.commit();
                prepared.releaseSuperseded();
            }
            assertValues(root, handle, first, second);
            try (ListValue.PreparedMutation prepared = root.preparePop(handle, 1, true)) {
                prepared.commit();
                prepared.releaseSuperseded();
            }
            assertValues(root, handle, second);
            root.release(handle);
            Assert.assertEquals(0L, backend.stats().liveObjects());
        }
    }

    @Test
    public void preparedQuicklistPushCanBeAbortedAndCommittedAtBothEdges() {
        try (TestBackend runtime = TestBackend.open("list-value-prepared-quicklist-push");
             StableMemoryBackend backend = runtime.backend();
             ListRoot root = new ListRoot(backend)) {
            byte[] first = filled('m', 5_000);
            byte[] second = filled('n', 5_000);
            ValueHandle handle = root.build(List.of(first, second));
            long liveBefore = backend.stats().liveObjects();

            try (ListValue.PreparedMutation prepared = root.preparePush(
                    handle,
                    List.of(filled('a', 5_000)),
                    true
            )) {
                Assert.assertEquals(ListValue.PreparedMutation.Operation.QUICKLIST_PUSH, prepared.operation());
            }
            Assert.assertEquals(liveBefore, backend.stats().liveObjects());
            assertValues(root, handle, first, second);

            byte[] left = filled('l', 5_000);
            try (ListValue.PreparedMutation prepared = root.preparePush(handle, List.of(left), true)) {
                prepared.commit();
                prepared.releaseSuperseded();
            }
            byte[] right = filled('r', 5_000);
            try (ListValue.PreparedMutation prepared = root.preparePush(handle, List.of(right), false)) {
                prepared.commit();
                prepared.releaseSuperseded();
            }

            assertValues(root, handle, left, first, second, right);
            root.release(handle);
            Assert.assertEquals(0L, backend.stats().liveObjects());
        }
    }

    @Test
    public void releasingQuicklistPushRefreshesRetainedHeapAccounting() {
        try (TestBackend runtime = TestBackend.open("list-value-prepared-metadata-refresh");
             StableMemoryBackend backend = runtime.backend();
             ListRoot root = new ListRoot(backend)) {
            byte[] first = filled('a', 5_000);
            byte[] second = filled('b', 5_000);
            ValueHandle handle = root.build(List.of(b("tail")));
            try {
                try (ListValue.PreparedMutation prepared = root.preparePush(
                        handle,
                        List.of(first, second),
                        true
                )) {
                    prepared.commit();
                    prepared.releaseSuperseded();
                }
                assertQuicklistMetadataLinks(root, handle, backend);
                long retainedBefore = root.retainedHeapBytes();
                try (ListValue.PreparedMutation prepared = root.preparePush(
                        handle,
                        List.of(filled('c', 5_000)),
                        true
                )) {
                    prepared.commit();
                    Assert.assertEquals(retainedBefore, root.retainedHeapBytes());
                    prepared.releaseSuperseded();
                }
                assertQuicklistMetadataLinks(root, handle, backend);
                try (ListValue.PreparedMutation prepared = root.preparePush(
                        handle,
                        List.of(filled('d', 5_000)),
                        false
                )) {
                    prepared.commit();
                    prepared.releaseSuperseded();
                }
                assertQuicklistMetadataLinks(root, handle, backend);
                try (ListValue.PreparedMutation prepared = root.preparePop(handle, 1, true)) {
                    prepared.commit();
                    prepared.releaseSuperseded();
                }
                assertQuicklistMetadataLinks(root, handle, backend);
                try (ListValue.PreparedMutation prepared = root.preparePop(handle, 1, false)) {
                    prepared.commit();
                    prepared.releaseSuperseded();
                }
                assertQuicklistMetadataLinks(root, handle, backend);
                Assert.assertTrue(root.retainedHeapBytes() > retainedBefore);
            } finally {
                root.release(handle);
            }
        }
    }

    @Test
    public void preparedPopAndRangeCoverNoopFullAndNormalizedBounds() {
        try (TestBackend runtime = TestBackend.open("list-value-bounds");
             ListRoot root = new ListRoot(runtime.backend())) {
            ValueHandle handle = root.build(List.of(b("a"), b("b"), b("c")));

            Assert.assertEquals(0, root.preparedPopNativeAllocationSizes(handle, 3, true).length);
            try (ListValue.PreparedMutation prepared = root.preparePop(handle, 0, true)) {
                Assert.assertEquals(3, prepared.size());
                prepared.commit();
                prepared.releaseSuperseded();
            }
            Assert.assertThrows(IllegalArgumentException.class, () -> root.preparePop(handle, 3, false));

            assertValues(root, handle, b("a"), b("b"), b("c"));
            Assert.assertEquals(3, root.rangeCount(handle, -100, 100));
            Assert.assertEquals(0, root.rangeCount(handle, 5, 9));
            Assert.assertEquals(0, root.rangeCount(handle, -1, -2));
            Assert.assertTrue(root.range(handle, 5, 9).isEmpty());
            Assert.assertTrue(root.range(handle, -1, -2).isEmpty());

            root.release(handle);
        }
    }

    @Test
    public void packedListPromotesToQuicklistWithoutChangingRootHandle() {
        try (TestBackend runtime = TestBackend.open("list-value-promotion")) {
            StableMemoryBackend backend = runtime.backend();
            ListRoot root = new ListRoot(backend);
            ValueHandle handle = root.create();
            NativeHandle identity = handle.nativeHandle();
            try {
                root.rpush(handle, List.of(b("a"), b("b"), b("c")));
                Assert.assertEquals(3, root.size(handle));
                root.rpush(handle, List.of(new byte[4096], new byte[4096], new byte[4096]));
                Assert.assertEquals(identity, handle.nativeHandle());
            } finally {
                root.release(handle);
            }
        }
    }

    @Test
    public void pushPopPreservesOrderAndReleasesRemovedNodes() {
        try (TestBackend runtime = TestBackend.open("list-value-pop")) {
            StableMemoryBackend backend = runtime.backend();
            ListRoot root = new ListRoot(backend);
            ValueHandle handle = root.build(List.of(b("a"), b("b"), b("c")));
            try {
                Assert.assertArrayEquals(b("a"), root.lpop(handle, 1).get(0));
                Assert.assertArrayEquals(b("c"), root.rpop(handle, 1).get(0));
                List<byte[]> values = root.range(handle, 0, -1);
                Assert.assertEquals(1, values.size());
                Assert.assertArrayEquals(b("b"), values.get(0));
            } finally {
                root.release(handle);
            }
            Assert.assertEquals(0L, backend.stats().liveObjects());
        }
    }

    @Test
    public void preparedMutationVariantsReportConservativeHeapBounds() {
        try (TestBackend runtime = TestBackend.open("list-value-prepared")) {
            ListRoot root = new ListRoot(runtime.backend());
            ValueHandle packed = root.build(List.of(b("a"), b("b")));
            ValueHandle quicklist = root.build(List.of(filled('a', 5_000), filled('b', 5_000), b("tail")));
            try {
                List<byte[]> packedAddition = List.of(new byte[512], new byte[512]);
                try (ListValue.PreparedMutation prepared = root.preparePush(packed, packedAddition, false)) {
                    Assert.assertTrue(prepared.stagedHeapBytes()
                            <= root.estimatedPreparedPushHeapGrowthBytes(packed, packedAddition, false));
                }
                List<byte[]> promotion = List.of(filled('c', 5_000), filled('d', 5_000));
                try (ListValue.PreparedMutation prepared = root.preparePush(packed, promotion, false)) {
                    Assert.assertTrue(prepared.stagedHeapBytes()
                            <= root.estimatedPreparedPushHeapGrowthBytes(packed, promotion, false));
                }
                List<byte[]> quicklistAddition = List.of(filled('e', 5_000));
                try (ListValue.PreparedMutation prepared = root.preparePush(quicklist, quicklistAddition, true)) {
                    Assert.assertTrue(prepared.stagedHeapBytes()
                            <= root.estimatedPreparedPushHeapGrowthBytes(quicklist, quicklistAddition, true));
                }
                try (ListValue.PreparedMutation prepared = root.preparePop(quicklist, 1, false)) {
                    Assert.assertTrue(prepared.stagedHeapBytes()
                            <= root.estimatedPreparedPopHeapGrowthBytes(quicklist, 1, false));
                }
                try (ListValue.PreparedMutation prepared = root.preparePop(quicklist, 0, true)) {
                    Assert.assertTrue(prepared.stagedHeapBytes()
                            <= root.estimatedPreparedPopHeapGrowthBytes(quicklist, 0, true));
                }
            } finally {
                root.release(packed);
                root.release(quicklist);
            }
        }
    }

    @Test
    public void releasedListRootHandleCannotBeUsedAfterFurtherAllocations() {
        try (TestBackend runtime = TestBackend.open("list-value-stale")) {
            StableMemoryBackend backend = runtime.backend();
            ListRoot root = new ListRoot(backend);
            ValueHandle first = root.create();
            NativeHandle firstIdentity = first.nativeHandle();
            root.release(first);
            ValueHandle second = root.create();
            try {
                Assert.assertNotEquals(firstIdentity, second.nativeHandle());
                Assert.assertFalse(root.contains(first));
                Assert.assertThrows(RuntimeException.class, () -> root.size(first));
            } finally {
                root.release(second);
            }
        }
    }

    @Test
    public void preparedMutationRejectsReleaseBeforeCommitAndDoubleCommit() {
        try (TestBackend runtime = TestBackend.open("list-prepared-state-guards");
             StableMemoryBackend backend = runtime.backend();
             ListRoot root = new ListRoot(backend)) {
            ValueHandle handle = root.build(List.of(b("a")));
            try {
                try (ListValue.PreparedMutation prepared = root.preparePush(
                        handle,
                        List.of(b("b")),
                        false
                )) {
                    Assert.assertThrows(IllegalStateException.class, prepared::releaseSuperseded);
                    prepared.commit();
                    Assert.assertThrows(IllegalStateException.class, prepared::commit);
                    prepared.releaseSuperseded();
                    prepared.releaseSuperseded();
                }
                assertValues(root, handle, b("a"), b("b"));
            } finally {
                root.release(handle);
            }
            Assert.assertEquals(0L, backend.stats().liveObjects());
        }
    }

    @Test
    public void closingPreparedPromotionReleasesStagedObjectsAndPreventsCommit() {
        try (TestBackend runtime = TestBackend.open("list-prepared-abort-cleanup");
             StableMemoryBackend backend = runtime.backend();
             ListRoot root = new ListRoot(backend)) {
            ValueHandle handle = root.build(List.of(b("tail")));
            try {
                long liveBefore = backend.stats().liveObjects();
                ListValue.PreparedMutation prepared = root.preparePush(
                        handle,
                        List.of(filled('a', 5_000), filled('b', 5_000)),
                        true
                );
                Assert.assertEquals(ValueEncoding.LIST_QUICKLIST, prepared.encoding());
                Assert.assertTrue(backend.stats().liveObjects() > liveBefore);

                prepared.close();
                prepared.close();

                Assert.assertEquals(liveBefore, backend.stats().liveObjects());
                Assert.assertThrows(IllegalStateException.class, prepared::commit);
                Assert.assertThrows(IllegalStateException.class, prepared::releaseSuperseded);
                assertValues(root, handle, b("tail"));
            } finally {
                root.release(handle);
            }
            Assert.assertEquals(0L, backend.stats().liveObjects());
        }
    }

    @Test
    public void preparedPackedMutationRejectsAChangedSourceTopologyAndCleansItsStage() {
        try (TestBackend runtime = TestBackend.open("list-prepared-stale-source");
             StableMemoryBackend backend = runtime.backend();
             ListRoot root = new ListRoot(backend)) {
            ValueHandle handle = root.build(List.of(b("a")));
            try {
                long liveBefore = backend.stats().liveObjects();
                try (ListValue.PreparedMutation prepared = root.preparePush(
                        handle,
                        List.of(b("b")),
                        false
                )) {
                    root.rpush(handle, List.of(b("c")));
                    Assert.assertThrows(IllegalStateException.class, prepared::commit);
                }

                assertValues(root, handle, b("a"), b("c"));
                Assert.assertEquals(liveBefore, backend.stats().liveObjects());
            } finally {
                root.release(handle);
            }
        }
    }

    private static byte[] filled(char value, int length) {
        byte[] bytes = new byte[length];
        java.util.Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static void assertValues(ListRoot root, ValueHandle handle, byte[]... expected) {
        List<byte[]> actual = root.range(handle, 0, -1);
        Assert.assertEquals(expected.length, actual.size());
        for (int index = 0; index < expected.length; index++) {
            Assert.assertArrayEquals(expected[index], actual.get(index));
        }
    }

    private static void assertQuicklistMetadataLinks(
            ListRoot root,
            ValueHandle handle,
            StableMemoryBackend backend
    ) {
        List<NativeHandle> nodes = new ArrayList<>();
        root.forEachNativeHandle(handle, nativeHandle -> {
            try (NativeObjectView view = backend.resolve(nativeHandle, NativeAccessMode.READ_ONLY)) {
                if (view.size() == QUICKLIST_NODE_RECORD_BYTES
                        && handle.nativeHandle().equals(readHandle(view, QUICKLIST_NODE_OWNER_ROOT_OFFSET))) {
                    nodes.add(nativeHandle);
                }
            }
        });
        Assert.assertTrue(nodes.size() >= 2);
        for (int index = 0; index < nodes.size(); index++) {
            NativeHandle expectedPrevious = index == 0 ? NativeHandle.NULL : nodes.get(index - 1);
            NativeHandle expectedNext = index + 1 == nodes.size()
                    ? NativeHandle.NULL
                    : nodes.get(index + 1);
            try (NativeObjectView view = backend.resolve(nodes.get(index), NativeAccessMode.READ_ONLY)) {
                Assert.assertEquals(expectedPrevious, readHandle(view, QUICKLIST_NODE_PREV_OFFSET));
                Assert.assertEquals(expectedNext, readHandle(view, QUICKLIST_NODE_NEXT_OFFSET));
            }
        }
    }

    private static NativeHandle readHandle(NativeObjectView view, int offset) {
        return new NativeHandle(view.getLongLittleEndian(offset), view.getLongLittleEndian(offset + Long.BYTES));
    }
}
