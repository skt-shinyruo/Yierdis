package yier.bubu.redis.storage.memory.internal.entry;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandle;

public class EntryHandleContractTest {
    @Test
    public void entryHandleRetainsTheCompleteNativeHandle() {
        NativeHandle nativeHandle = new NativeHandle(11L, 7L);

        EntryHandle handle = new EntryHandle(nativeHandle);

        Assert.assertEquals(nativeHandle, handle.nativeHandle());
        Assert.assertEquals(handle, new EntryHandle(nativeHandle));
    }

    @Test
    public void entryHandlesDoNotAliasEqualLocalRawValuesFromDifferentBackends() {
        EntryHandle left = new EntryHandle(new NativeHandle(11L, 1L));
        EntryHandle right = new EntryHandle(new NativeHandle(12L, 1L));

        Assert.assertNotEquals(left, right);
        Assert.assertEquals(left.nativeHandle().localRaw(), right.nativeHandle().localRaw());
        Assert.assertNotEquals(left.nativeHandle().allocatorId(), right.nativeHandle().allocatorId());
    }
}
