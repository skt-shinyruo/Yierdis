package yier.bubu.redis.memory.api;

import org.junit.Assert;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
    public void nullRequiresBothIdentityPartsToBeZero() {
        Assert.assertTrue(NativeHandle.NULL.isNull());
        Assert.assertEquals(0L, NativeHandle.NULL.allocatorId());
        Assert.assertEquals(0L, NativeHandle.NULL.localRaw());
        Assert.assertFalse(new NativeHandle(1L, 0L).isNull());
        Assert.assertFalse(new NativeHandle(0L, 1L).isNull());
    }

    @Test
    public void equalityIncludesAllocatorIdentity() {
        NativeHandle first = new NativeHandle(11L, 77L);
        NativeHandle same = new NativeHandle(11L, 77L);
        NativeHandle otherAllocator = new NativeHandle(12L, 77L);

        Assert.assertEquals(first, same);
        Assert.assertNotEquals(first, otherAllocator);
    }

    @Test
    public void backendIdsArePositiveAndStrictlyMonotonic() {
        long first = StableMemoryBackendIds.nextId();
        long second = StableMemoryBackendIds.nextId();
        long third = StableMemoryBackendIds.nextId();

        Assert.assertTrue(first > 0L);
        Assert.assertTrue(second > first);
        Assert.assertTrue(third > second);
    }

    @Test
    public void exhaustedBackendIdsStayExhausted() {
        Assert.assertEquals(2L, StableMemoryBackendIds.advance(1L));
        Assert.assertEquals(0L, StableMemoryBackendIds.advance(0L));
        Assert.assertEquals(0L, StableMemoryBackendIds.advance(Long.MAX_VALUE));
    }

    @Test
    public void backendIdsArePositiveAndUniqueAcrossConcurrentCallers() throws Exception {
        int workerCount = 8;
        int idsPerWorker = 1_000;
        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<List<Long>>> futures = new ArrayList<>();
            for (int worker = 0; worker < workerCount; worker++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    List<Long> generated = new ArrayList<>(idsPerWorker);
                    for (int index = 0; index < idsPerWorker; index++) {
                        generated.add(StableMemoryBackendIds.nextId());
                    }
                    return generated;
                }));
            }
            start.countDown();

            Set<Long> unique = new HashSet<>();
            for (Future<List<Long>> future : futures) {
                for (long id : future.get(5L, TimeUnit.SECONDS)) {
                    Assert.assertTrue(id > 0L);
                    Assert.assertTrue("duplicate backend ID " + id, unique.add(id));
                }
            }
            Assert.assertEquals(workerCount * idsPerWorker, unique.size());
        } finally {
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
        }
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
