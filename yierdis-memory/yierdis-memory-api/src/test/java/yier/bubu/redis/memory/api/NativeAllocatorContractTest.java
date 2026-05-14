package yier.bubu.redis.memory.api;

import org.junit.Assert;
import org.junit.Test;

public class NativeAllocatorContractTest {
    @Test
    public void statsRecordExposesAllocatorCounters() {
        NativeAllocatorStats stats = new NativeAllocatorStats(
                10,
                64,
                128,
                64,
                54,
                2,
                3,
                4,
                2,
                1,
                3,
                4,
                5,
                6
        );

        Assert.assertEquals(10L, stats.logicalUsedBytes());
        Assert.assertEquals(64L, stats.reservedBytes());
        Assert.assertEquals(128L, stats.committedBytes());
        Assert.assertEquals(64L, stats.freeBytes());
        Assert.assertEquals(54L, stats.internalFragmentationBytes());
        Assert.assertEquals(2L, stats.liveSmallPages());
        Assert.assertEquals(3L, stats.liveMediumSpanPages());
        Assert.assertEquals(4L, stats.liveLargeSpanPages());
        Assert.assertEquals(2L, stats.liveObjects());
        Assert.assertEquals(1L, stats.pinnedObjects());
        Assert.assertEquals(3L, stats.quarantinedObjects());
        Assert.assertEquals(4L, stats.staleHandleDetections());
        Assert.assertEquals(5L, stats.reallocInPlaceCount());
        Assert.assertEquals(6L, stats.reallocMovedCount());
    }

    @Test
    public void exceptionTypesCarryMessages() {
        NativeMemoryException base = new NativeMemoryException("base");
        StaleNativeHandleException stale = new StaleNativeHandleException("stale");

        Assert.assertEquals("base", base.getMessage());
        Assert.assertEquals("stale", stale.getMessage());
    }
}
