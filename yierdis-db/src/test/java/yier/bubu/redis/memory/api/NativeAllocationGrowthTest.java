package yier.bubu.redis.memory.api;

import org.junit.Assert;
import org.junit.Test;

public class NativeAllocationGrowthTest {
    @Test
    public void effectiveBytesSaturate() {
        NativeAllocationGrowth max = new NativeAllocationGrowth(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(Long.MAX_VALUE, max.effectiveBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNegativeHeapGrowth() {
        new NativeAllocationGrowth(-1, 0, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNegativeMetadataGrowth() {
        new NativeAllocationGrowth(0, -1, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNegativeDataGrowth() {
        new NativeAllocationGrowth(0, 0, -1);
    }
}
