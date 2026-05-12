package yier.bubu.redis.storage.memory.internal.entry;

import org.junit.Assert;
import org.junit.Test;

public class ValueHandleContractTest {
    @Test
    public void valueHandleExposesRawIdentityAndRecordEquality() {
        ValueHandle left = new ValueHandle(42L);
        ValueHandle same = new ValueHandle(42L);
        ValueHandle different = new ValueHandle(43L);

        Assert.assertEquals(42L, left.raw());
        Assert.assertEquals(left, same);
        Assert.assertEquals(left.hashCode(), same.hashCode());
        Assert.assertNotEquals(left, different);
    }

    @Test
    public void valueHandlePreservesSentinelRawValues() {
        Assert.assertEquals(0L, new ValueHandle(0L).raw());
        Assert.assertEquals(-1L, new ValueHandle(-1L).raw());
        Assert.assertEquals(Long.MAX_VALUE, new ValueHandle(Long.MAX_VALUE).raw());
    }
}
