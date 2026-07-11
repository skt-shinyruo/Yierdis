package yier.bubu.redis.memory.api;

import org.junit.Assert;
import org.junit.Test;

public class NativeAllocationGrowthTest {
    @Test
    public void effectiveBytesAndPlusSaturate() {
        NativeAllocationGrowth max = new NativeAllocationGrowth(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(Long.MAX_VALUE, max.effectiveBytes());
        Assert.assertEquals(max, max.plus(new NativeAllocationGrowth(1, 2, 3)));
    }

    @Test
    public void zeroIsTheAdditiveIdentity() {
        NativeAllocationGrowth growth = new NativeAllocationGrowth(1, 2, 3);
        Assert.assertEquals(growth, growth.plus(NativeAllocationGrowth.zero()));
        Assert.assertEquals(growth, NativeAllocationGrowth.zero().plus(growth));
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
