package yier.bubu.redis.memory.api;

import org.junit.Assert;
import org.junit.Test;

public class StableMemoryBackendContractTest {
    @Test
    public void statsRecordExposesProductionAllocatorCounters() {
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
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
                18,
                19,
                20,
                21
        );

        Assert.assertEquals(9L, stats.externalFragmentationBytes());
        Assert.assertEquals(10L, stats.smallFreeBytes());
        Assert.assertEquals(11L, stats.mediumFreeBytes());
        Assert.assertEquals(12L, stats.largeFreeBytes());
        Assert.assertEquals(13L, stats.freePages());
        Assert.assertEquals(14L, stats.quarantineBytes());
        Assert.assertEquals(15L, stats.doubleFreeDetections());
        Assert.assertEquals(16L, stats.defragReclaimedPages());
    }

    @Test
    public void defragCycleRecordsExposeBudgetsAndCounters() {
        NativeDefragOptions options = new NativeDefragOptions(64, 3, 1_000);
        Assert.assertEquals(64L, options.maxMoveBytes());
        Assert.assertEquals(3L, options.maxObjects());
        Assert.assertEquals(1_000L, options.timeBudgetNanos());

        NativeDefragReport report = new NativeDefragReport(
                4,
                2,
                48,
                1,
                1,
                1,
                true,
                false,
                true
        );
        Assert.assertEquals(4L, report.scannedObjects());
        Assert.assertEquals(2L, report.movedObjects());
        Assert.assertEquals(48L, report.movedBytes());
        Assert.assertEquals(1L, report.skippedPinnedObjects());
        Assert.assertEquals(1L, report.skippedBudgetObjects());
        Assert.assertEquals(1L, report.failedMoves());
        Assert.assertTrue(report.stoppedByByteBudget());
        Assert.assertFalse(report.stoppedByObjectBudget());
        Assert.assertTrue(report.stoppedByTimeBudget());
    }

    @Test
    public void exceptionTypesCarryMessages() {
        NativeMemoryException base = new NativeMemoryException("base");
        StaleNativeHandleException stale = new StaleNativeHandleException("stale");

        Assert.assertEquals("base", base.getMessage());
        Assert.assertEquals("stale", stale.getMessage());
    }

}
