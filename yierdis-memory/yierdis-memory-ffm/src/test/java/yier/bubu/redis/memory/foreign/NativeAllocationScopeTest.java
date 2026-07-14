package yier.bubu.redis.memory.foreign;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAllocationScope;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;

public class NativeAllocationScopeTest {
    @Test
    public void abortedNativeScopeRestoresCommittedSnapshot() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("scope-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 128)) {
            allocator.bindToCurrentThread();
            NativeHandle warmPageValue = allocator.allocate(NativeObjectKind.STRING_BYTES, 32);
            allocator.free(warmPageValue);
            MemoryUsageSnapshot before = allocator.memoryUsage();
            try (NativeAllocationScope scope = allocator.beginAllocationScope()) {
                allocator.allocate(NativeObjectKind.STRING_BYTES, 32);
                allocator.allocate(NativeObjectKind.ENTRY_RECORD, 56);
                for (int i = 0; i < 20; i++) {
                    allocator.allocate(NativeObjectKind.STRING_BYTES, 70_000);
                }
                Assert.assertTrue(scope.growth().effectiveBytes() > 0);
                scope.abort();
            }
            Assert.assertEquals(before, allocator.memoryUsage());
            Assert.assertEquals(0L, allocator.stats().liveObjects());
        }
    }

    @Test
    public void scopedSnapshotCountsCheckpointAndScopeBookkeepingHeap() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("scope-bookkeeping-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 128)) {
            allocator.bindToCurrentThread();
            NativeHandle warmPageValue = allocator.allocate(NativeObjectKind.STRING_BYTES, 32);
            allocator.free(warmPageValue);
            MemoryUsageSnapshot before = allocator.memoryUsage();
            try (NativeAllocationScope scope = allocator.beginAllocationScope()) {
                MemoryUsageSnapshot scoped = allocator.memoryUsage();
                Assert.assertTrue(scoped.heapEstimatedBytes() > before.heapEstimatedBytes());
                Assert.assertTrue(scope.growth().heapEstimatedBytes() > 0L);
            }
            Assert.assertEquals(before, allocator.memoryUsage());
            Assert.assertEquals(0L, allocator.stats().liveObjects());
        }
    }

    @Test
    public void scopeBookkeepingEstimateCoversLargeTrackedHandleArray() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("scope-bookkeeping-estimate-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 2_048)) {
            allocator.bindToCurrentThread();
            NativeHandle anchor = allocator.allocate(NativeObjectKind.STRING_BYTES, 32);
            try {
                MemoryUsageSnapshot before = allocator.memoryUsage();
                int allocationCount = 1_024;
                long expectedBookkeeping = allocator.estimateAllocationScopeBookkeepingBytes(allocationCount);
                Assert.assertTrue(expectedBookkeeping > 4_096L);

                try (NativeAllocationScope scope = allocator.beginAllocationScope()) {
                    for (int i = 0; i < allocationCount; i++) {
                        allocator.allocate(NativeObjectKind.STRING_BYTES, 32);
                    }
                    Assert.assertEquals(expectedBookkeeping, scope.growth().heapEstimatedBytes());
                    scope.abort();
                }

                Assert.assertEquals(before, allocator.memoryUsage());
            } finally {
                allocator.free(anchor);
            }
        }
    }

    @Test
    public void abortDoesNotAllocateWhileRecoveringPageDirectoryIds() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("scope-abort-directory-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 128)) {
            allocator.bindToCurrentThread();
            MemoryUsageSnapshot before = allocator.memoryUsage();
            NativeAllocationScope scope = allocator.beginAllocationScope();
            for (int i = 0; i < 17; i++) {
                allocator.allocate(NativeObjectKind.STRING_BYTES, 70_000);
            }

            allocator.armAllocationScopeAbortAllocationTrackingForTesting();
            try {
                scope.abort();
            } finally {
                allocator.disarmAllocationScopeAbortAllocationTrackingForTesting();
            }

            Assert.assertFalse(allocator.allocationScopeAbortAllocatedForTesting());
            Assert.assertEquals(before, allocator.memoryUsage());
        }
    }

    @Test
    public void memorySnapshotUsesRetainedAllocatorCountersWithoutWalkingPagesOrSegments() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("scope-o1-snapshot-test");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 256)) {
            allocator.bindToCurrentThread();
            NativeHandle[] handles = new NativeHandle[64];
            for (int i = 0; i < handles.length; i++) {
                handles[i] = allocator.allocate(NativeObjectKind.STRING_BYTES, 70_000);
            }

            allocator.armMemoryUsageIterationTrapsForTesting();
            try {
                Assert.assertTrue(allocator.memoryUsage().heapEstimatedBytes() > 0L);
            } finally {
                allocator.disarmMemoryUsageIterationTrapsForTesting();
                for (NativeHandle handle : handles) {
                    allocator.free(handle);
                }
            }
        }
    }
}
