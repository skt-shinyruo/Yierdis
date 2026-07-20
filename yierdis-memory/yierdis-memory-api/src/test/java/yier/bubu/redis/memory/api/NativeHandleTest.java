package yier.bubu.redis.memory.api;

import org.junit.Assert;
import org.junit.Test;

import java.lang.management.ManagementFactory;

public class NativeHandleTest {
    @Test
    public void domainLookupDoesNotAllocateForEveryDecodedHandle() {
        com.sun.management.ThreadMXBean bean = allocatedBytesBean();
        int domainCount = NativeHandleDomain.values().length;
        for (int index = 0; index < 10_000; index++) {
            Assert.assertNotNull(NativeHandleDomain.fromCode(index % domainCount));
        }

        long before = bean.getThreadAllocatedBytes(Thread.currentThread().threadId());
        for (int index = 0; index < 100_000; index++) {
            Assert.assertNotNull(NativeHandleDomain.fromCode(index % domainCount));
        }
        long allocatedBytes = bean.getThreadAllocatedBytes(Thread.currentThread().threadId()) - before;

        Assert.assertTrue("domain lookup allocated " + allocatedBytes + " bytes", allocatedBytes < 4_096L);
    }

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
        Assert.assertEquals(0x1100_075b_cd15_04d3L, handle.raw());
        Assert.assertEquals(NativeHandleDomain.STORAGE_OBJECT, handle.domain());
        Assert.assertEquals(NativeObjectKind.STRING_BYTES.code(), handle.kindCode());
        Assert.assertEquals(123456789L, handle.slotId());
        Assert.assertEquals(77, handle.generation());
        Assert.assertEquals(3, handle.flags());
    }

    @Test
    public void primitiveDecodersMatchHandleAccessorsWithoutPerCallAllocation() {
        NativeHandle handle = NativeHandle.of(
                NativeHandleDomain.STORAGE_OBJECT,
                NativeObjectKind.STRING_BYTES,
                123456789L,
                77,
                3
        );
        long raw = handle.raw();
        Assert.assertEquals(raw, NativeHandle.rawOf(
                NativeHandleDomain.STORAGE_OBJECT,
                NativeObjectKind.STRING_BYTES,
                123456789L,
                77,
                3
        ));

        com.sun.management.ThreadMXBean bean = allocatedBytesBean();
        for (int index = 0; index < 10_000; index++) {
            consumeRawFields(raw);
        }
        long before = bean.getThreadAllocatedBytes(Thread.currentThread().threadId());
        for (int index = 0; index < 100_000; index++) {
            consumeRawFields(raw);
        }
        long allocatedBytes = bean.getThreadAllocatedBytes(Thread.currentThread().threadId()) - before;

        Assert.assertEquals(handle.domain(), NativeHandle.domain(raw));
        Assert.assertEquals(handle.kindCode(), NativeHandle.kindCode(raw));
        Assert.assertEquals(handle.slotId(), NativeHandle.slotId(raw));
        Assert.assertEquals(handle.generation(), NativeHandle.generation(raw));
        Assert.assertEquals(handle.flags(), NativeHandle.flags(raw));
        Assert.assertTrue("primitive handle decoding allocated " + allocatedBytes + " bytes", allocatedBytes < 4_096L);
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
    public void rejectsMismatchedDomainAndKind() {
        assertIllegal(() -> NativeHandle.of(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.ENTRY_RECORD, 1, 1, 0));
    }

    @Test
    public void collectionNativeObjectKindsHaveDistinctCodesInsideTheirDomains() {
        java.util.EnumMap<NativeHandleDomain, java.util.HashSet<Integer>> seen = new java.util.EnumMap<>(NativeHandleDomain.class);
        for (NativeObjectKind kind : NativeObjectKind.values()) {
            java.util.HashSet<Integer> codes = seen.computeIfAbsent(kind.domain(), ignored -> new java.util.HashSet<>());
            Assert.assertTrue("duplicate kind code " + kind.code() + " in domain " + kind.domain(), codes.add(kind.code()));
        }

        Assert.assertEquals(NativeHandleDomain.TYPE_ROOT, NativeObjectKind.LIST_ROOT.domain());
        Assert.assertEquals(NativeHandleDomain.TYPE_ROOT, NativeObjectKind.HASH_ROOT.domain());
        Assert.assertEquals(NativeHandleDomain.TYPE_ROOT, NativeObjectKind.SET_ROOT.domain());
        Assert.assertEquals(NativeHandleDomain.TYPE_ROOT, NativeObjectKind.ZSET_ROOT.domain());
        Assert.assertEquals(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.LISTPACK_BYTES.domain());
        Assert.assertEquals(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.HASH_FIELD_BYTES.domain());
        Assert.assertEquals(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.HASH_VALUE_BYTES.domain());
        Assert.assertEquals(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.SET_MEMBER_BYTES.domain());
        Assert.assertEquals(NativeHandleDomain.STORAGE_OBJECT, NativeObjectKind.ZSET_MEMBER_BYTES.domain());
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

    private static long consumeRawFields(long raw) {
        return NativeHandle.domainCode(raw)
                + NativeHandle.kindCode(raw)
                + NativeHandle.slotId(raw)
                + NativeHandle.generation(raw)
                + NativeHandle.flags(raw);
    }

    private static com.sun.management.ThreadMXBean allocatedBytesBean() {
        java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        Assert.assertTrue("thread allocation accounting is unavailable", bean instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocatedBytesBean = (com.sun.management.ThreadMXBean) bean;
        Assert.assertTrue("thread allocation accounting is unsupported", allocatedBytesBean.isThreadAllocatedMemorySupported());
        if (!allocatedBytesBean.isThreadAllocatedMemoryEnabled()) {
            allocatedBytesBean.setThreadAllocatedMemoryEnabled(true);
        }
        return allocatedBytesBean;
    }
}
