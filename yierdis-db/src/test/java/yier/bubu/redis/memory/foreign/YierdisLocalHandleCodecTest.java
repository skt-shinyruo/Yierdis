package yier.bubu.redis.memory.foreign;

import java.lang.management.ManagementFactory;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.NativeHandleDomain;
import yier.bubu.redis.memory.api.NativeObjectKind;

public class YierdisLocalHandleCodecTest {
    @Test
    public void encodesAndDecodesHandleFields() {
        long localRaw = YierdisLocalHandleCodec.encode(
                NativeHandleDomain.STORAGE_OBJECT,
                NativeObjectKind.STRING_BYTES,
                123456789L,
                77,
                3
        );

        YierdisLocalHandleCodec.requireValid(localRaw);
        Assert.assertEquals(0x1100_075b_cd15_04d3L, localRaw);
        Assert.assertEquals(
                NativeHandleDomain.STORAGE_OBJECT,
                YierdisLocalHandleCodec.domain(localRaw)
        );
        Assert.assertEquals(
                NativeObjectKind.STRING_BYTES.code(),
                YierdisLocalHandleCodec.kindCode(localRaw)
        );
        Assert.assertEquals(123456789L, YierdisLocalHandleCodec.slotId(localRaw));
        Assert.assertEquals(77, YierdisLocalHandleCodec.generation(localRaw));
        Assert.assertEquals(3, YierdisLocalHandleCodec.flags(localRaw));
    }

    @Test
    public void primitiveLocalDecodersDoNotAllocatePerCall() {
        long localRaw = YierdisLocalHandleCodec.encode(
                NativeHandleDomain.STORAGE_OBJECT,
                NativeObjectKind.STRING_BYTES,
                123456789L,
                77,
                3
        );

        com.sun.management.ThreadMXBean bean = allocatedBytesBean();
        for (int index = 0; index < 10_000; index++) {
            consumeLocalFields(localRaw);
        }
        long before = bean.getThreadAllocatedBytes(Thread.currentThread().threadId());
        for (int index = 0; index < 100_000; index++) {
            consumeLocalFields(localRaw);
        }
        long allocatedBytes = bean.getThreadAllocatedBytes(Thread.currentThread().threadId()) - before;

        Assert.assertEquals(
                NativeHandleDomain.STORAGE_OBJECT,
                YierdisLocalHandleCodec.domain(localRaw)
        );
        Assert.assertEquals(
                NativeObjectKind.STRING_BYTES.code(),
                YierdisLocalHandleCodec.kindCode(localRaw)
        );
        Assert.assertEquals(123456789L, YierdisLocalHandleCodec.slotId(localRaw));
        Assert.assertEquals(77, YierdisLocalHandleCodec.generation(localRaw));
        Assert.assertEquals(3, YierdisLocalHandleCodec.flags(localRaw));
        Assert.assertTrue(
                "primitive local-handle decoding allocated " + allocatedBytes + " bytes",
                allocatedBytes < 4_096L
        );
    }

    @Test
    public void rejectsOutOfRangeFields() {
        assertIllegal(() -> encode(-1L, 1, 0));
        assertIllegal(() -> encode(1L << 40, 1, 0));
        assertIllegal(() -> encode(1L, -1, 0));
        assertIllegal(() -> encode(1L, 4096, 0));
        assertIllegal(() -> encode(1L, 1, -1));
        assertIllegal(() -> encode(1L, 1, 16));
    }

    @Test
    public void rejectsMismatchedDomainAndKind() {
        assertIllegal(() -> YierdisLocalHandleCodec.encode(
                NativeHandleDomain.STORAGE_OBJECT,
                NativeObjectKind.ENTRY_RECORD,
                1L,
                1,
                0
        ));
    }

    @Test
    public void rejectsNonZeroReservedDomain() {
        assertIllegal(() -> YierdisLocalHandleCodec.requireValid(0x10L));
    }

    private static long encode(long slotId, int generation, int flags) {
        return YierdisLocalHandleCodec.encode(
                NativeHandleDomain.STORAGE_OBJECT,
                NativeObjectKind.STRING_BYTES,
                slotId,
                generation,
                flags
        );
    }

    private static void assertIllegal(Runnable runnable) {
        try {
            runnable.run();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            Assert.assertNotNull(expected.getMessage());
        }
    }

    private static long consumeLocalFields(long localRaw) {
        return YierdisLocalHandleCodec.domain(localRaw).code()
                + YierdisLocalHandleCodec.kindCode(localRaw)
                + YierdisLocalHandleCodec.slotId(localRaw)
                + YierdisLocalHandleCodec.generation(localRaw)
                + YierdisLocalHandleCodec.flags(localRaw);
    }

    private static com.sun.management.ThreadMXBean allocatedBytesBean() {
        java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        Assert.assertTrue(
                "thread allocation accounting is unavailable",
                bean instanceof com.sun.management.ThreadMXBean
        );
        com.sun.management.ThreadMXBean allocatedBytesBean =
                (com.sun.management.ThreadMXBean) bean;
        Assert.assertTrue(
                "thread allocation accounting is unsupported",
                allocatedBytesBean.isThreadAllocatedMemorySupported()
        );
        if (!allocatedBytesBean.isThreadAllocatedMemoryEnabled()) {
            allocatedBytesBean.setThreadAllocatedMemoryEnabled(true);
        }
        return allocatedBytesBean;
    }
}
