package yier.bubu.redis.memory.foreign;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAllocationScope;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.StableMemoryBackendIds;

public class NativeAllocationScopeTest {
    @Test
    public void abortedNativeScopeRestoresCommittedSnapshot() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("scope-test");
             YierdisStableNativeAllocator allocator = newAllocator(runtime, 128)) {
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
             YierdisStableNativeAllocator allocator = newAllocator(runtime, 128)) {
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
    public void openingScopeDoesNotCopyRetainedAllocatorDirectories() {
        long oneSegmentOverhead = scopeOpeningHeapOverhead(1);
        long expandedDirectoryOverhead = scopeOpeningHeapOverhead(9);

        Assert.assertTrue(
                "scope setup must not copy retained allocator directories",
                expandedDirectoryOverhead <= oneSegmentOverhead + 32L
        );
    }

    @Test
    public void promotedCheckpointsReleaseCowedDirectoryReferences() {
        YierdisNativePageDirectory directory = new YierdisNativePageDirectory();
        directory.add(new Object());
        YierdisNativePageDirectory.AllocationScopeCheckpoint checkpoint = directory.allocationScopeCheckpoint();

        directory.add(new Object());
        long duringScope = checkpoint.heapEstimatedBytes();
        directory.promoteAllocationScope(checkpoint);

        Assert.assertTrue(checkpoint.heapEstimatedBytes() < duringScope);
    }

    @Test
    public void promotedObjectTableCheckpointReleasesCowedDirectoryReferences() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("scope-promote-release-test");
             YierdisNativeObjectTable table = new YierdisNativeObjectTable(
                     runtime,
                     128,
                     0,
                     (pageId, pageOffset, pageClass) -> 1
             )) {
            YierdisNativeObjectTable.AllocationScopeCheckpoint checkpoint = table.allocationScopeCheckpoint();
            table.allocate(NativeObjectKind.STRING_BYTES, 1, 1, 0, 0L, 0, 0L);
            long duringScope = checkpoint.heapEstimatedBytes();

            table.promoteAllocationScope(checkpoint);

            Assert.assertTrue(checkpoint.heapEstimatedBytes() < duringScope);
        }
    }

    @Test
    public void scopeBookkeepingEstimateCoversLargeTrackedHandleArray() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("scope-bookkeeping-estimate-test");
             YierdisStableNativeAllocator allocator = newAllocator(runtime, 2_048)) {
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
                    Assert.assertTrue(scope.growth().heapEstimatedBytes() > 4_096L);
                    Assert.assertTrue(expectedBookkeeping >= scope.growth().heapEstimatedBytes());
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
             YierdisStableNativeAllocator allocator = newAllocator(runtime, 128)) {
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
    public void abortDoesNotGrowTheObjectTableAvailabilityQueue() {
        int allocationCount = YierdisNativeObjectSegment.SLOTS_PER_SEGMENT * 2;
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("scope-abort-object-table-test");
             YierdisStableNativeAllocator allocator = newAllocator(runtime, allocationCount)) {
            allocator.bindToCurrentThread();
            MemoryUsageSnapshot before = allocator.memoryUsage();
            NativeAllocationScope scope = allocator.beginAllocationScope();
            for (int i = 0; i < allocationCount; i++) {
                allocator.allocate(NativeObjectKind.STRING_BYTES, 1);
            }

            scope.abort();

            Assert.assertEquals(before, allocator.memoryUsage());
            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 1);
            allocator.free(handle);
            Assert.assertEquals(0L, allocator.stats().liveObjects());
        }
    }

    @Test
    public void automaticCapacityAbortReleasesNewMetadataSegment() {
        int firstSegmentSlots = YierdisNativeObjectSegment.SLOTS_PER_SEGMENT;
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("scope-auto-segment-abort-test");
             YierdisStableNativeAllocator allocator = newAllocator(runtime, 0)) {
            allocator.bindToCurrentThread();
            NativeHandle[] retained = new NativeHandle[firstSegmentSlots];
            for (int i = 0; i < retained.length; i++) {
                retained[i] = allocator.allocate(NativeObjectKind.STRING_BYTES, 1);
            }
            MemoryUsageSnapshot before = allocator.memoryUsage();
            Assert.assertEquals(1L, allocator.metadataStats().activeMetadataSegments());

            try (NativeAllocationScope scope = allocator.beginAllocationScope()) {
                allocator.allocate(NativeObjectKind.STRING_BYTES, 1);
                Assert.assertEquals(2L, allocator.metadataStats().activeMetadataSegments());
                scope.abort();
            }

            Assert.assertEquals(before, allocator.memoryUsage());
            Assert.assertEquals(1L, allocator.metadataStats().activeMetadataSegments());
            for (NativeHandle handle : retained) {
                allocator.free(handle);
            }
        }
    }

    @Test
    public void growthRetainsTransientNativePeakAfterTheHandleIsFreed() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("scope-transient-peak-test");
             YierdisStableNativeAllocator allocator = newAllocator(runtime, 128)) {
            allocator.bindToCurrentThread();
            try (NativeAllocationScope scope = allocator.beginAllocationScope()) {
                NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 70_000);
                long peakNativeDataBytes = scope.growth().nativeDataCommittedBytes();
                Assert.assertTrue(peakNativeDataBytes > 0L);

                allocator.free(handle);

                Assert.assertEquals(peakNativeDataBytes, scope.growth().nativeDataCommittedBytes());
            }
        }
    }

    @Test
    public void memorySnapshotUsesRetainedAllocatorCountersWithoutWalkingPagesOrSegments() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("scope-o1-snapshot-test");
             YierdisStableNativeAllocator allocator = newAllocator(runtime, 256)) {
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

    private static long scopeOpeningHeapOverhead(int segmentCount) {
        int allocationCount = segmentCount * YierdisNativeObjectSegment.SLOTS_PER_SEGMENT;
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("scope-directory-overhead-" + segmentCount);
             YierdisStableNativeAllocator allocator = newAllocator(runtime, allocationCount)) {
            allocator.bindToCurrentThread();
            NativeHandle[] handles = new NativeHandle[allocationCount];
            for (int i = 0; i < handles.length; i++) {
                handles[i] = allocator.allocate(NativeObjectKind.STRING_BYTES, 1);
            }
            for (NativeHandle handle : handles) {
                allocator.free(handle);
            }

            MemoryUsageSnapshot before = allocator.memoryUsage();
            try (NativeAllocationScope scope = allocator.beginAllocationScope()) {
                long overhead = allocator.memoryUsage().heapEstimatedBytes() - before.heapEstimatedBytes();
                scope.abort();
                return overhead;
            }
        }
    }

    private static YierdisStableNativeAllocator newAllocator(
            YierdisFfmMemoryRuntime runtime,
            int maxSlots
    ) {
        YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(
                runtime,
                maxSlots,
                StableMemoryBackendIds.nextId(),
                new FfmTestOwner()
        );
        allocator.bindToCurrentThread();
        return allocator;
    }
}
