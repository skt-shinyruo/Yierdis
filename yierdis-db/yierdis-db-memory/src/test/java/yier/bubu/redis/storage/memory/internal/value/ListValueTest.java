package yier.bubu.redis.storage.memory.internal.value;

import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.memory.TestBackend;
import yier.bubu.redis.storage.memory.internal.entry.ListRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;

import static yier.bubu.redis.storage.testkit.TestBytes.b;

public class ListValueTest {
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
                Assert.assertTrue(backend.stats().objectCount(NativeObjectKind.LIST_NODE) > 0L);
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
                int allocationCount = root.preparedPushNativeAllocationSizes(handle, addition, false).length;
                long upperBound = root.estimatedPreparedPushHeapGrowthBytes(
                        handle,
                        addition,
                        false,
                        allocationCount
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
}
