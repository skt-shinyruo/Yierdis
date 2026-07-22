package yier.bubu.redis.memory.foreign;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    public void byteAccessDoesNotAllocateFfmSpans() {
        com.sun.management.ThreadMXBean bean = allocatedBytesBean();
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("block-access-allocation");
             YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {
            YierdisNativeBlock block = allocator.allocate(16);
            try {
                for (int index = 0; index < 10_000; index++) {
                    block.setByte(0, (byte) index);
                    block.getByte(0);
                }

                long before = bean.getThreadAllocatedBytes(Thread.currentThread().threadId());
                int checksum = 0;
                for (int index = 0; index < 100_000; index++) {
                    block.setByte(0, (byte) index);
                    checksum += block.getByte(0);
                }
                long allocatedBytes = bean.getThreadAllocatedBytes(Thread.currentThread().threadId()) - before;

                Assert.assertTrue("block byte access allocated " + allocatedBytes + " bytes", allocatedBytes < 1L);
                Assert.assertNotEquals(0, checksum);
            } finally {
                block.close();
            }
        }
    }

    @Test
    public void pageDirectoryReportsCheckedIdExhaustion() throws Exception {
        YierdisNativePageDirectory directory = new YierdisNativePageDirectory();
        Field nextId = YierdisNativePageDirectory.class.getDeclaredField("nextId");
        nextId.setAccessible(true);
        nextId.setInt(directory, -1);

        try {
            directory.add(new Object());
            Assert.fail("expected page id exhaustion");
        } catch (OffHeapOutOfMemoryException expected) {
            Assert.assertTrue(expected.getMessage().contains("page id"));
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
    public void failedScopeRestoreDoesNotLeavePageDirectoryScopeActive() {
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
    public void trimUsesTheEmptyPageIndexAndHonorsInspectionBudget() {
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
    public void allocationEstimateIncludesOnlyNewSegmentsAndPages() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("allocation-estimate");
             YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(
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
            long baselineDirectoryBytes = allocator.stats().pageDirectoryHeapBytes();
            for (int i = 0; i < 100_000; i++) {
                allocator.allocate((i & 1) == 0 ? 32_769 : 1_048_577).close();
            }
            Assert.assertEquals(0L, allocator.stats().livePageDirectoryEntries());
            Assert.assertEquals(0L, allocator.stats().liveSpanDescriptors());
            Assert.assertEquals(baselineDirectoryBytes, allocator.stats().pageDirectoryHeapBytes());
            Assert.assertEquals(0L, runtime.liveRegionCount());
        }
    }

    @Test
    public void allocatorsDoNotRetainHistoricalCollectionsOrRegions() {
        Assert.assertFalse(Arrays.stream(YierdisNativePageAllocator.class.getDeclaredFields())
                .map(Field::getType)
                .anyMatch(type -> List.class.isAssignableFrom(type) || Map.class.isAssignableFrom(type)));
        Assert.assertFalse(Arrays.stream(YierdisFfmMemoryRuntime.class.getDeclaredFields())
                .map(Field::getType)
                .anyMatch(type -> Set.class.isAssignableFrom(type) || Map.class.isAssignableFrom(type)));
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
    public void sizeClassLookupDoesNotCloneEnumValuesOnTheAllocationPath() {
        com.sun.management.ThreadMXBean bean = allocatedBytesBean();
        long checksum = 0L;
        for (int index = 0; index < 10_000; index++) {
            checksum += YierdisNativeSizeClass.forSize(1 + index % 32_768).bytes();
        }

        long before = bean.getThreadAllocatedBytes(Thread.currentThread().threadId());
        for (int index = 0; index < 100_000; index++) {
            checksum += YierdisNativeSizeClass.forSize(1 + index % 32_768).bytes();
        }
        long allocatedBytes = bean.getThreadAllocatedBytes(Thread.currentThread().threadId()) - before;

        Assert.assertNotEquals(0L, checksum);
        Assert.assertTrue("size-class lookup allocated " + allocatedBytes + " bytes", allocatedBytes < 4_096L);
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

            Assert.assertEquals(YierdisNativeSizeClass.forSize(16), first.sizeClass());
            Assert.assertEquals(YierdisNativeSizeClass.forSize(17), second.sizeClass());
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
            Assert.assertEquals(32769, block.requestedBytes());
            Assert.assertEquals(YierdisNativePageAllocator.PAGE_BYTES, block.capacity());
            Assert.assertEquals(1, block.pageCount());
        }
    }

    @Test
    public void allocatesLargeSpansForObjectsAboveOneMiB() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("native-large");
             YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {

            YierdisNativeBlock block = allocator.allocate((1024 * 1024) + 1);

            Assert.assertEquals(YierdisNativePageClass.LARGE_SPAN, block.pageClass());
            Assert.assertEquals((1024 * 1024) + 1, block.requestedBytes());
            Assert.assertEquals(17, block.pageCount());
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

    private static com.sun.management.ThreadMXBean allocatedBytesBean() {
        java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        Assert.assertTrue("thread allocation accounting is unavailable", bean instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocatedBytesBean = (com.sun.management.ThreadMXBean) bean;
        Assert.assertTrue("thread allocation accounting is unsupported", allocatedBytesBean.isThreadAllocatedMemorySupported());
        if (!allocatedBytesBean.isThreadAllocatedMemoryEnabled()) {
            allocatedBytesBean.setThreadAllocatedMemoryEnabled(true);
        }
        return allocatedBytesBean;
    }
}
