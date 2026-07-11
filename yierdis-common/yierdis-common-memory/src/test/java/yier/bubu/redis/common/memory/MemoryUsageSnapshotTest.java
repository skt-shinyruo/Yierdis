package yier.bubu.redis.common.memory;

import org.junit.Assert;
import org.junit.Test;

public class MemoryUsageSnapshotTest {
    @Test
    public void effectiveBytesUsePhysicalCommittedMemory() {
        MemoryUsageSnapshot usage = new MemoryUsageSnapshot(7, 11, 13, 5, 8);

        Assert.assertEquals(31L, usage.effectiveBytesForMaxmemory());
    }

    @Test
    public void aggregationSaturatesInsteadOfWrapping() {
        MemoryUsageSnapshot left = new MemoryUsageSnapshot(Long.MAX_VALUE, 0, 0, 0, 0);
        MemoryUsageSnapshot right = new MemoryUsageSnapshot(1, 2, 3, 4, 5);

        MemoryUsageSnapshot total = left.plus(right);

        Assert.assertEquals(Long.MAX_VALUE, total.heapEstimatedBytes());
        Assert.assertEquals(Long.MAX_VALUE, total.effectiveBytesForMaxmemory());
    }

    @Test(expected = IllegalArgumentException.class)
    public void negativeComponentsAreRejected() {
        new MemoryUsageSnapshot(-1, 0, 0, 0, 0);
    }

    @Test
    public void unlimitedPressureBudgetUsesSaturatingLimits() {
        MemoryPressureBudget budget = MemoryPressureBudget.unlimited();

        Assert.assertEquals(Long.MAX_VALUE, budget.maxInspectedUnits());
        Assert.assertEquals(Long.MAX_VALUE, budget.maxReclaimedBytes());
        Assert.assertEquals(Long.MAX_VALUE, budget.timeLimitNanos());
    }

    @Test(expected = IllegalArgumentException.class)
    public void pressureBudgetRejectsNegativeLimits() {
        new MemoryPressureBudget(0, -1, 0);
    }

    @Test
    public void emptyReclaimResultIsCompleteAndZero() {
        MemoryReclaimResult result = MemoryReclaimResult.empty();

        Assert.assertEquals(0L, result.inspectedUnits());
        Assert.assertEquals(0L, result.reclaimedUnits());
        Assert.assertEquals(0L, result.reclaimedBytes());
        Assert.assertEquals(MemoryReclaimResult.StopReason.COMPLETE, result.stopReason());
    }

    @Test
    public void reclaimResultRequiresStopReason() {
        Assert.assertThrows(NullPointerException.class, () -> new MemoryReclaimResult(0, 0, 0, null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void reclaimResultRejectsNegativeCounters() {
        new MemoryReclaimResult(0, -1, 0, MemoryReclaimResult.StopReason.COMPLETE);
    }
}
