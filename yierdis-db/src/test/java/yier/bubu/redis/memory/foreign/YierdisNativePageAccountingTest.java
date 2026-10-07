package yier.bubu.redis.memory.foreign;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAllocationGrowth;
import yier.bubu.redis.memory.api.NativeAllocationScope;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.StableMemoryBackendFactory;
import yier.bubu.redis.memory.api.StableMemoryBackendIds;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.storage.memory.YierdisDbEngineFactory;

public class YierdisNativePageAccountingTest {
    private static final int PAGE_BYTES = YierdisNativePageAllocator.PAGE_BYTES;
    private static final int FULL_SMALL_BLOCK = 32_768;
    private static final int LIVE_SMALL_PAGES = 64;

    @Test
    public void scopedAllocateAndFreeDoesNotWalkLivePages() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("page-accounting-hot-path");
             YierdisFfmStableMemoryBackend backend = backend(runtime, LIVE_SMALL_PAGES * 2 + 8)) {
            List<NativeHandle> held = new ArrayList<>(LIVE_SMALL_PAGES * 2);
            try {
                for (int i = 0; i < LIVE_SMALL_PAGES * 2; i++) {
                    held.add(backend.allocate(NativeObjectKind.STRING_BYTES, FULL_SMALL_BLOCK));
                }
                Assert.assertEquals(LIVE_SMALL_PAGES, backend.stats().liveSmallPages());

                long visits = backend.fullPageSummaryVisits();
                try (NativeAllocationScope scope = backend.beginAllocationScope()) {
                    NativeHandle extra = backend.allocate(NativeObjectKind.STRING_BYTES, 16);
                    backend.free(extra);
                    scope.growth();
                    scope.promote();
                }
                backend.memoryUsage();
                backend.stats();
                Assert.assertEquals(visits, backend.fullPageSummaryVisits());
            } finally {
                for (NativeHandle handle : held) {
                    backend.free(handle);
                }
            }
        }
    }

    @Test
    public void smallAllocationGrowthMatchesCommittedAndUsedDelta() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("page-accounting-delta");
             YierdisFfmStableMemoryBackend backend = backend(runtime, 32)) {
            MemoryUsageSnapshot before = backend.memoryUsage();
            try (NativeAllocationScope scope = backend.beginAllocationScope()) {
                NativeHandle handle = backend.allocate(NativeObjectKind.STRING_BYTES, 16);
                NativeAllocationGrowth growth = scope.growth();
                MemoryUsageSnapshot during = backend.memoryUsage();

                Assert.assertEquals(PAGE_BYTES, during.nativeDataCommittedBytes() - before.nativeDataCommittedBytes());
                Assert.assertEquals(16L, during.nativeDataLiveBytes() - before.nativeDataLiveBytes());
                Assert.assertEquals(PAGE_BYTES, growth.nativeDataCommittedBytes());
                assertPageCountersMatchAudit(backend);
                scope.promote();
                backend.free(handle);
            }
            MemoryUsageSnapshot afterFree = backend.memoryUsage();
            // 释放后留下一页 warm page，committed 仍是这一页；used 回到分配前。
            Assert.assertEquals(PAGE_BYTES, afterFree.nativeDataCommittedBytes() - before.nativeDataCommittedBytes());
            Assert.assertEquals(0L, afterFree.nativeDataLiveBytes() - before.nativeDataLiveBytes());
            Assert.assertEquals(1L, backend.stats().emptySmallPages());
            assertPageCountersMatchAudit(backend);
        }
    }

    @Test
    public void abortedScopeLeavesCountersMatchingLivePages() {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("page-accounting-abort");
             YierdisFfmStableMemoryBackend backend = backend(runtime, 64)) {
            NativeHandle anchor = backend.allocate(NativeObjectKind.STRING_BYTES, 32);
            MemoryUsageSnapshot before = backend.memoryUsage();
            try (NativeAllocationScope scope = backend.beginAllocationScope()) {
                backend.allocate(NativeObjectKind.STRING_BYTES, 16);
                backend.allocate(NativeObjectKind.STRING_BYTES, 24);
                for (int i = 0; i < 4; i++) {
                    backend.allocate(NativeObjectKind.STRING_BYTES, 70_000);
                }
                scope.abort();
            }

            Assert.assertEquals(before, backend.memoryUsage());
            Assert.assertEquals(1L, backend.stats().liveObjects());
            assertPageCountersMatchAudit(backend);
            backend.free(anchor);
            assertPageCountersMatchAudit(backend);
        }
    }

    @Test
    public void syncFlushDbDoesNotSummarizePagesPerKey() {
        int keyCount = 2_048;
        try (FfmDb empty = openFfmDb();
             FfmDb filled = openFfmDb()) {
            NativeAllocatorStats emptyStats = empty.backend().stats();
            MemoryUsageSnapshot emptyUsage = empty.backend().memoryUsage();
            for (int i = 0; i < keyCount; i++) {
                filled.db().strings().setString(
                        ("k" + i).getBytes(StandardCharsets.UTF_8),
                        ("v" + i).getBytes(StandardCharsets.UTF_8),
                        SetMode.NORMAL,
                        null
                );
            }
            long livePages = livePages(filled.backend().stats());
            Assert.assertTrue(livePages > 1L);

            long visits = filled.backend().fullPageSummaryVisits();
            Assert.assertEquals(MutationOutcome.VALUE_CHANGED, filled.db().flushDb());
            long visitedDuringFlush = filled.backend().fullPageSummaryVisits() - visits;
            Assert.assertEquals(0L, visitedDuringFlush);

            assertPageCountersMatchAudit(filled.backend());
            NativeAllocatorStats flushed = filled.backend().stats();
            MemoryUsageSnapshot flushedUsage = filled.backend().memoryUsage();
            long emptyPageDelta = flushed.emptySmallPages() - emptyStats.emptySmallPages();
            Assert.assertEquals(0L, flushedUsage.nativeDataLiveBytes() - emptyUsage.nativeDataLiveBytes());
            Assert.assertEquals(emptyPageDelta, livePages(flushed) - livePages(emptyStats));
            Assert.assertEquals(emptyPageDelta * PAGE_BYTES, flushed.committedBytes() - emptyStats.committedBytes());
        }
    }

    private static void assertPageCountersMatchAudit(YierdisFfmStableMemoryBackend backend) {
        NativeAllocatorStats maintained = backend.stats();
        YierdisNativePageAllocatorStats audited = backend.auditedPageStats();
        Assert.assertEquals(audited.committedBytes(), maintained.committedBytes());
        Assert.assertEquals(audited.usedBytes(), backend.memoryUsage().nativeDataLiveBytes());
        Assert.assertEquals(audited.smallFreeBytes(), maintained.smallFreeBytes());
        Assert.assertEquals(audited.emptySmallPages(), maintained.emptySmallPages());
        Assert.assertEquals(audited.liveSmallPages(), maintained.liveSmallPages());
        Assert.assertEquals(audited.liveMediumPages(), maintained.liveMediumPages());
        Assert.assertEquals(audited.liveLargePages(), maintained.liveLargePages());
        Assert.assertEquals(audited.freeBytes(), maintained.freeBytes());
    }

    private static long livePages(NativeAllocatorStats stats) {
        return stats.liveSmallPages() + stats.liveMediumPages() + stats.liveLargePages();
    }

    private static YierdisFfmStableMemoryBackend backend(YierdisFfmMemoryRuntime runtime, int maxSlots) {
        YierdisFfmStableMemoryBackend backend = new YierdisFfmStableMemoryBackend(
                runtime,
                maxSlots,
                StableMemoryBackendIds.nextId(),
                new FfmTestOwner()
        );
        backend.bindToCurrentThread();
        return backend;
    }

    private static FfmDb openFfmDb() {
        List<YierdisFfmStableMemoryBackend> created = new ArrayList<>(1);
        StableMemoryBackendFactory factory = (name, maxSlots, owner) -> {
            YierdisFfmStableMemoryBackend backend = new YierdisFfmStableMemoryBackend(name, maxSlots, owner);
            created.add(backend);
            return backend;
        };
        YierdisDb db = new YierdisDbEngineFactory(factory, 0).create(new DbEngineConfig(
                0,
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                0L,
                0L,
                new DbDefragConfig(false, 0L, 0L, 0L)
        ));
        db.bindToCurrentThread();
        Assert.assertEquals(1, created.size());
        return new FfmDb(db, created.get(0));
    }

    private record FfmDb(YierdisDb db, YierdisFfmStableMemoryBackend backend) implements AutoCloseable {
        @Override
        public void close() {
            db.shutdown();
        }
    }
}
