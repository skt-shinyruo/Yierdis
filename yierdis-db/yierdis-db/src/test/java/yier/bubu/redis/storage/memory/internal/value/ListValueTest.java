package yier.bubu.redis.storage.memory.internal.value;

import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.memory.internal.entry.ListRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class ListValueTest {
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
    public void preparedPushReportsAConservativeHeapBound() {
        try (TestBackend runtime = TestBackend.open("list-value-prepared")) {
            ListRoot root = new ListRoot(runtime.backend());
            ValueHandle handle = root.build(List.of(b("a"), b("b")));
            try {
                List<byte[]> addition = List.of(new byte[512], new byte[512]);
                long upperBound = root.estimatedPreparedPushHeapGrowthBytes(
                        handle,
                        addition,
                        false
                );
                try (ListValue.PreparedMutation prepared = root.preparePush(handle, addition, false)) {
                    Assert.assertTrue(prepared.stagedHeapBytes() <= upperBound);
                }
            } finally {
                root.release(handle);
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
}
