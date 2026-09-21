package yier.bubu.redis.storage.memory.internal.entry;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandle;

public class ValueHandleContractTest {
    @Test
    public void valueHandleRetainsTheCompleteNativeHandle() {
        NativeHandle nativeHandle = new NativeHandle(42L, 3L);
        ValueHandle left = new ValueHandle(nativeHandle);
        ValueHandle same = new ValueHandle(nativeHandle);
        ValueHandle different = new ValueHandle(new NativeHandle(43L, 3L));

        Assert.assertEquals(nativeHandle, left.nativeHandle());
        Assert.assertEquals(left, same);
        Assert.assertEquals(left.hashCode(), same.hashCode());
        Assert.assertNotEquals(left, different);
    }

    @Test
    public void valueHandleAllowsNativeNullOnlyAsSentinel() {
        Assert.assertEquals(NativeHandle.NULL, ValueHandle.NULL.nativeHandle());
        Assert.assertTrue(ValueHandle.NULL.nativeHandle().isNull());
    }
}
