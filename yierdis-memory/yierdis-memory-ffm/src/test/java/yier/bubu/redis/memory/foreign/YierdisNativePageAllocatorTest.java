package yier.bubu.redis.memory.foreign;

import java.lang.reflect.Field;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.memory.api.NativeAllocationGrowth;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.OffHeapOutOfMemoryException;
import yier.bubu.redis.memory.api.StableMemoryBackendIds;

public class YierdisNativePageAllocatorTest {
    @Test
    public void blockByteAccessRoundTripsWithinCapacity() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("block-access-allocation");
             YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {
            YierdisNativeBlock block = allocator.allocate(16);
            try {
                block.setByte(0, (byte) 17);
                block.setByte(block.capacity() - 1, (byte) 29);

                Assert.assertEquals(17, block.getByte(0));
                Assert.assertEquals(29, block.getByte(block.capacity() - 1));
                Assert.assertThrows(IndexOutOfBoundsException.class, () -> block.getByte(block.capacity()));
            } finally {
                block.close();
            }
        }
    }

    @Test
    public void reusablePageIdIsConsumedBeforeCheckedIdExhaustion() throws Exception {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("page-id-exhaustion");
             YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {
            YierdisNativeBlock released = allocator.allocate(70_000);
            int reusableId = released.pageId();
            released.close();

            Field nextPageId = YierdisNativePageAllocator.class.getDeclaredField("nextPageId");
            nextPageId.setAccessible(true);
            nextPageId.setInt(allocator, -1);

            YierdisNativeBlock reused = allocator.allocate(70_000);
            Assert.assertEquals(reusableId, reused.pageId());
            OffHeapOutOfMemoryException failure = Assert.assertThrows(
                    OffHeapOutOfMemoryException.class,
                    () -> allocator.allocate(70_000)
            );
            Assert.assertTrue(failure.getMessage().contains("page id"));
            reused.close();
        }
    }

    @Test
    public void normalFreeRetainsOneWarmPageAndPressureTrimClosesIt() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("page-trim");
             YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {
            YierdisNativeBlock block = allocator.allocate(32);
            block.close();
            Assert.assertEquals(1L, allocator.stats().emptySmallPages());
            Assert.assertEquals(YierdisNativePageAllocator.PAGE_BYTES, runtime.usedBytes());

            MemoryReclaimResult result = allocator.trimEmptyPages(MemoryPressureBudget.unlimited());
            Assert.assertEquals(1L, result.reclaimedUnits());
            Assert.assertEquals(YierdisNativePageAllocator.PAGE_BYTES, result.reclaimedBytes());
            Assert.assertEquals(0L, runtime.usedBytes());
        }
    }

    @Test
    public void failedScopeRestoreDoesNotLeavePageRegistryScopeActive() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("failed-scope-restore");
             YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {
            YierdisNativePageAllocator.AllocationScopeCheckpoint checkpoint = allocator.beginAllocationScope();
            YierdisNativeBlock span = allocator.allocate(32_769);
            allocator.beginAllocationScopeAbort(checkpoint);

            Assert.assertThrows(IllegalStateException.class, () -> allocator.restoreAllocationScope(checkpoint));
            span.close();

            YierdisNativePageAllocator.AllocationScopeCheckpoint next = allocator.beginAllocationScope();
            allocator.promoteAllocationScope(next);
        }
    }

    @Test
    public void trimScansEmptyPagesInPageIdOrderAndHonorsInspectionBudget() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("bounded-trim");
             YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {
            YierdisNativeBlock first = allocator.allocate(16);
            YierdisNativeBlock second = allocator.allocate(32);
            first.close();
            second.close();
            MemoryReclaimResult result = allocator.trimEmptyPages(
                    new MemoryPressureBudget(1, Long.MAX_VALUE, Long.MAX_VALUE)
            );
            Assert.assertEquals(1L, result.inspectedUnits());
            Assert.assertEquals(YierdisNativePageAllocator.PAGE_BYTES, result.reclaimedBytes());
            Assert.assertEquals(MemoryReclaimResult.StopReason.INSPECTION_LIMIT, result.stopReason());
        }
    }

    @Test
    public void trimPreservesInspectionTimeAndByteStopReasons() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("trim-stop-reasons");
             YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {
            allocator.allocate(16).close();

            MemoryReclaimResult inspection = allocator.trimEmptyPages(new MemoryPressureBudget(0, 0, 0));
            Assert.assertEquals(MemoryReclaimResult.StopReason.INSPECTION_LIMIT, inspection.stopReason());
            Assert.assertEquals(0L, inspection.inspectedUnits());

            MemoryReclaimResult time = allocator.trimEmptyPages(
                    new MemoryPressureBudget(Long.MAX_VALUE, Long.MAX_VALUE, 0)
            );
            Assert.assertEquals(MemoryReclaimResult.StopReason.TIME_LIMIT, time.stopReason());
            Assert.assertEquals(0L, time.inspectedUnits());

            MemoryReclaimResult bytes = allocator.trimEmptyPages(
                    new MemoryPressureBudget(Long.MAX_VALUE, 0, Long.MAX_VALUE)
            );
            Assert.assertEquals(MemoryReclaimResult.StopReason.BYTE_LIMIT, bytes.stopReason());
            Assert.assertEquals(1L, bytes.inspectedUnits());
            Assert.assertEquals(0L, bytes.reclaimedUnits());
        }
    }

    @Test
    public void allocationEstimateIncludesOnlyNewSegmentsAndPages() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("allocation-estimate");
             YierdisFfmStableMemoryBackend allocator = new YierdisFfmStableMemoryBackend(
                     runtime,
                     8_192,
                     StableMemoryBackendIds.nextId(),
                     new FfmTestOwner()
             )) {
            allocator.bindToCurrentThread();
            NativeAllocationGrowth first = allocator.estimateAdditionalGrowth(32);
            Assert.assertEquals(4_096L * YierdisNativeObjectTable.META_BYTES,
                    first.nativeMetadataCommittedBytes());
            Assert.assertEquals(YierdisNativePageAllocator.PAGE_BYTES,
                    first.nativeDataCommittedBytes());
            Assert.assertTrue(first.heapEstimatedBytes() > 0L);
            NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 32);
            Assert.assertEquals(NativeAllocationGrowth.zero(), allocator.estimateAdditionalGrowth(32));
            allocator.free(handle);
        }
    }

    @Test
    public void spanChurnDoesNotRetainHistoricalDescriptorsOrPageIds() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("span-churn");
             YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {
            long baselineRegistryBytes = allocator.stats().pageRegistryHeapBytes();
            for (int i = 0; i < 100_000; i++) {
                allocator.allocate((i & 1) == 0 ? 32_769 : 1_048_577).close();
            }
            Assert.assertEquals(0L, allocator.stats().livePageRegistryEntries());
            Assert.assertEquals(0L, allocator.stats().liveSpanDescriptors());
            Assert.assertEquals(
                    baselineRegistryBytes + 64L,
                    allocator.stats().pageRegistryHeapBytes()
            );
            Assert.assertEquals(0L, runtime.liveRegionCount());
        }
    }

    @Test
    public void pageRegistryReusesTheLowestReleasedId() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("page-id-order");
             YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {
            YierdisNativeBlock first = allocator.allocate(70_000);
            YierdisNativeBlock second = allocator.allocate(70_000);
            YierdisNativeBlock retained = allocator.allocate(70_000);
            int firstId = first.pageId();
            int secondId = second.pageId();
            second.close();
            first.close();

            YierdisNativeBlock reused = allocator.allocate(70_000);
            Assert.assertEquals(Math.min(firstId, secondId), reused.pageId());
            reused.close();
            retained.close();
        }
    }

    @Test
    public void reusedPageIdStillValidatesTheNewPageClass() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("page-id-class-validation");
             YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {
            YierdisNativeBlock small = allocator.allocate(17);
            int pageId = small.pageId();
            int pageOffset = small.pageOffset();
            small.close();
            allocator.trimEmptyPages(MemoryPressureBudget.unlimited());

            YierdisNativeBlock span = allocator.allocate(70_000);
            Assert.assertEquals(pageId, span.pageId());
            Assert.assertThrows(
                    IllegalStateException.class,
                    () -> allocator.resolveCapacity(
                            pageId,
                            pageOffset,
                            YierdisNativePageClass.SMALL.ordinal()
                    )
            );
            span.close();
        }
    }

    @Test
    public void choosesSmallSizeClasses() {
        Assert.assertEquals(16, YierdisNativeSizeClass.forSize(1).bytes());
        Assert.assertEquals(16, YierdisNativeSizeClass.forSize(16).bytes());
        Assert.assertEquals(24, YierdisNativeSizeClass.forSize(17).bytes());
        Assert.assertEquals(32, YierdisNativeSizeClass.forSize(25).bytes());
        Assert.assertEquals(1024, YierdisNativeSizeClass.forSize(769).bytes());
        Assert.assertEquals(32768, YierdisNativeSizeClass.forSize(32768).bytes());

        try {
            YierdisNativeSizeClass.forSize(32769);
            Assert.fail("expected non-small size rejection");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("small size"));
        }
    }

    @Test
    public void resolvesCapacityFromPageAndSpanDescriptors() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-capacity-resolver");
             YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {
            YierdisNativeBlock small = allocator.allocate(17);
            YierdisNativeBlock span = allocator.allocate(70_000);
            try {
                Assert.assertEquals(
                        small.capacity(),
                        allocator.resolveCapacity(
                                small.pageId(),
                                small.pageOffset(),
                                small.pageClass().ordinal()
                        )
                );
                Assert.assertEquals(
                        span.capacity(),
                        allocator.resolveCapacity(
                                span.pageId(),
                                span.pageOffset(),
                                span.pageClass().ordinal()
                        )
                );
                Assert.assertThrows(
                        IllegalStateException.class,
                        () -> allocator.resolveCapacity(
                                span.pageId(),
                                1,
                                span.pageClass().ordinal()
                        )
                );
            } finally {
                small.close();
                span.close();
            }
        }
    }

    @Test
    public void smallAllocationsNeverCrossPageBoundary() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-page-boundary");
             YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {

            for (int i = 0; i < 4096; i++) {
                YierdisNativeBlock block = allocator.allocate(17);
                Assert.assertEquals(YierdisNativePageClass.SMALL, block.pageClass());
                Assert.assertEquals(24, block.capacity());
                Assert.assertEquals(0, block.pageOffset() % block.capacity());
                Assert.assertTrue(block.pageOffset() + block.capacity() <= YierdisNativePageAllocator.PAGE_BYTES);
            }
        }
    }

    @Test
    public void smallPagesBelongToOneSizeClass() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-page-class");
             YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {

            YierdisNativeBlock first = allocator.allocate(16);
            YierdisNativeBlock second = allocator.allocate(17);

            Assert.assertEquals(YierdisNativeSizeClass.forSize(16).bytes(), first.capacity());
            Assert.assertEquals(YierdisNativeSizeClass.forSize(17).bytes(), second.capacity());
            Assert.assertNotEquals(first.pageId(), second.pageId());
        }
    }

    @Test
    public void freesAndReusesSmallBlocks() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-page-reuse");
             YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {

            YierdisNativeBlock first = allocator.allocate(32);
            int pageId = first.pageId();
            int pageOffset = first.pageOffset();
            first.close();

            YierdisNativeBlock second = allocator.allocate(32);
            Assert.assertEquals(pageId, second.pageId());
            Assert.assertEquals(pageOffset, second.pageOffset());
            Assert.assertEquals(32L, allocator.stats().usedBytes());
        }
    }

    @Test
    public void conservativeGrowthReusesAvailableSmallPageCapacity() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-page-conservative-growth");
             YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {

            YierdisNativeBlock existing = allocator.allocate(32);
            YierdisNativePageAllocator.PageGrowth projected = allocator.estimateConservativeAdditionalGrowth(32);

            Assert.assertEquals(0L, projected.nativeDataCommittedBytes());
            existing.close();
        }
    }

    @Test
    public void allocatesMediumSpansForObjectsAboveSmallLimit() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-medium");
             YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {

            YierdisNativeBlock block = allocator.allocate(32769);

            Assert.assertEquals(YierdisNativePageClass.MEDIUM_SPAN, block.pageClass());
            Assert.assertEquals(YierdisNativePageAllocator.PAGE_BYTES, block.capacity());
        }
    }

    @Test
    public void allocatesLargeSpansForObjectsAboveOneMiB() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-large");
             YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {

            YierdisNativeBlock block = allocator.allocate((1024 * 1024) + 1);

            Assert.assertEquals(YierdisNativePageClass.LARGE_SPAN, block.pageClass());
            Assert.assertEquals(17L * YierdisNativePageAllocator.PAGE_BYTES, block.capacity());
        }
    }

    @Test
    public void reportsCommittedUsedAndFreeBytes() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-page-stats");
             YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {

            YierdisNativeBlock small = allocator.allocate(16);
            YierdisNativeBlock medium = allocator.allocate(32769);

            YierdisNativePageAllocatorStats stats = allocator.stats();
            Assert.assertEquals(2L * YierdisNativePageAllocator.PAGE_BYTES, stats.committedBytes());
            Assert.assertEquals(16L + YierdisNativePageAllocator.PAGE_BYTES, stats.usedBytes());
            Assert.assertEquals((2L * YierdisNativePageAllocator.PAGE_BYTES) - 16L - YierdisNativePageAllocator.PAGE_BYTES,
                    stats.freeBytes());
            Assert.assertEquals(1L, stats.liveSmallPages());
            Assert.assertEquals(1L, stats.liveMediumSpanPages());
            Assert.assertEquals(0L, stats.liveLargeSpanPages());
            Assert.assertTrue(stats.smallFreeBytes() > 0L);
            Assert.assertEquals(0L, stats.mediumFreeBytes());
            Assert.assertEquals(0L, stats.largeFreeBytes());
            Assert.assertEquals(0L, stats.freePages());

            small.close();
            medium.close();

            YierdisNativePageAllocatorStats afterFree = allocator.stats();
            Assert.assertEquals(0L, afterFree.usedBytes());
            Assert.assertEquals(afterFree.committedBytes(), afterFree.freeBytes());
            Assert.assertEquals(YierdisNativePageAllocator.PAGE_BYTES, afterFree.smallFreeBytes());
            Assert.assertEquals(1L, afterFree.freePages());
        }
    }
}
