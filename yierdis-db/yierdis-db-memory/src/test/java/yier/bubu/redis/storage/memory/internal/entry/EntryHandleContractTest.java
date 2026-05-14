package yier.bubu.redis.storage.memory.internal.entry;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;

public class EntryHandleContractTest {
    @Test
    public void entryHandleWrapsProductionNativeHandle() {
        NativeHandle nativeHandle = NativeHandle.of(
                NativeObjectKind.ENTRY_RECORD.domain(),
                NativeObjectKind.ENTRY_RECORD,
                11L,
                7,
                0
        );
        EntryHandle handle = EntryHandle.fromNativeHandle(nativeHandle);

        Assert.assertEquals(nativeHandle.raw(), handle.raw());
        Assert.assertEquals(nativeHandle, handle.nativeHandle());
        Assert.assertEquals(handle, EntryHandle.fromNativeHandle(nativeHandle));
        Assert.assertNotEquals(
                handle,
                EntryHandle.fromNativeHandle(NativeHandle.of(
                        NativeObjectKind.ENTRY_RECORD.domain(),
                        NativeObjectKind.ENTRY_RECORD,
                        12L,
                        7,
                        0
                ))
        );
    }

    @Test
    public void entryHandleRejectsWrongNativeDomainOrKind() {
        NativeHandle wrong = NativeHandle.of(
                NativeObjectKind.STRING_BYTES.domain(),
                NativeObjectKind.STRING_BYTES,
                1L,
                1,
                0
        );

        try {
            EntryHandle.fromNativeHandle(wrong);
            Assert.fail("expected wrong entry handle kind rejection");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("ENTRY_RECORD"));
        }
    }
}
