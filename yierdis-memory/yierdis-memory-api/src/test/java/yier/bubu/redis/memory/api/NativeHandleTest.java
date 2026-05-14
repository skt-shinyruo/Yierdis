package yier.bubu.redis.memory.api;

import org.junit.Assert;
import org.junit.Test;

public class NativeHandleTest {
    @Test
    public void nullHandleIsOnlyZeroRawValue() {
        Assert.assertTrue(NativeHandle.NULL.isNull());
        Assert.assertEquals(0L, NativeHandle.NULL.raw());
    }

    @Test
    public void encodesAndDecodesHandleFields() {
        NativeHandle handle = NativeHandle.of(
                NativeHandleDomain.STORAGE_OBJECT,
                NativeObjectKind.STRING_BYTES,
                123456789L,
                77,
                3
        );

        Assert.assertFalse(handle.isNull());
        Assert.assertEquals(NativeHandleDomain.STORAGE_OBJECT, handle.domain());
        Assert.assertEquals(NativeObjectKind.STRING_BYTES.code(), handle.kindCode());
        Assert.assertEquals(123456789L, handle.slotId());
        Assert.assertEquals(77, handle.generation());
        Assert.assertEquals(3, handle.flags());
    }

    @Test
    public void rejectsOutOfRangeFields() {
        assertIllegal(() -> NativeHandle.of(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.STRING_BYTES, -1, 1, 0));
        assertIllegal(() -> NativeHandle.of(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.STRING_BYTES, 1L << 40, 1, 0));
        assertIllegal(() -> NativeHandle.of(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.STRING_BYTES, 1, -1, 0));
        assertIllegal(() -> NativeHandle.of(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.STRING_BYTES, 1, 4096, 0));
        assertIllegal(() -> NativeHandle.of(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.STRING_BYTES, 1, 1, -1));
        assertIllegal(() -> NativeHandle.of(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.STRING_BYTES, 1, 1, 16));
    }

    @Test
    public void rejectsNonZeroReservedDomain() {
        long raw = 0x0000_0000_0000_0010L;
        assertIllegal(() -> NativeHandle.fromRaw(raw));
    }

    private static void assertIllegal(Runnable runnable) {
        try {
            runnable.run();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            Assert.assertNotNull(expected.getMessage());
        }
    }
}
