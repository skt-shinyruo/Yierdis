package yier.bubu.redis.memory.foreign;

import org.junit.Assert;
import org.junit.Test;

public class YierdisNativePageAllocatorTest {
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
}
