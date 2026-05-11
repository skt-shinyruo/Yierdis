package yier.bubu.redis.storage.memory.internal.entry;

import org.junit.Assert;
import org.junit.Test;

public class EntryHandleContractTest {
    @Test
    public void entryHandleCarriesRawIdentity() {
        EntryHandle handle = new EntryHandle(11L);
        Assert.assertEquals(11L, handle.raw());
        Assert.assertEquals(handle, new EntryHandle(11L));
        Assert.assertNotEquals(handle, new EntryHandle(12L));
    }
}
