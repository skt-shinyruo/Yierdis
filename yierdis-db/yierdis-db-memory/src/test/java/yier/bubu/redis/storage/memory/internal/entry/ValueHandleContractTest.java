package yier.bubu.redis.storage.memory.internal.entry;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;

public class ValueHandleContractTest {
    @Test
    public void valueHandleWrapsProductionNativeHandle() {
        NativeHandle nativeHandle = NativeHandle.of(
                NativeObjectKind.STRING_BYTES.domain(),
                NativeObjectKind.STRING_BYTES,
                42L,
                3,
                0
        );
        ValueHandle left = ValueHandle.fromNativeHandle(nativeHandle);
        ValueHandle same = ValueHandle.fromNativeHandle(nativeHandle);
        ValueHandle different = ValueHandle.fromNativeHandle(NativeHandle.of(
                NativeObjectKind.STRING_BYTES.domain(),
                NativeObjectKind.STRING_BYTES,
                43L,
                3,
                0
        ));

        Assert.assertEquals(nativeHandle.raw(), left.raw());
        Assert.assertEquals(nativeHandle, left.nativeHandle());
        Assert.assertEquals(left, same);
        Assert.assertEquals(left.hashCode(), same.hashCode());
        Assert.assertNotEquals(left, different);
    }

    @Test
    public void valueHandleRejectsReservedNonNullRawValues() {
        try {
            ValueHandle.fromRaw(1L);
            Assert.fail("expected raw reserved-domain rejection");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("reserved domain"));
        }
    }

    @Test
    public void valueHandleAllowsNativeNullOnlyAsSentinel() {
        Assert.assertEquals(0L, ValueHandle.NULL.raw());
        Assert.assertTrue(ValueHandle.NULL.nativeHandle().isNull());
    }
}
