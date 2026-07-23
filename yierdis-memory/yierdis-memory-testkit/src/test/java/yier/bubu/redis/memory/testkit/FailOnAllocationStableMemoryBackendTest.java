package yier.bubu.redis.memory.testkit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocationGrowth;
import yier.bubu.redis.memory.api.NativeAllocationScope;
import yier.bubu.redis.memory.api.NativeAllocatorMetadataStats;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeCapacityExceededException;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.memory.api.NativeDefragResult;
import yier.bubu.redis.memory.api.NativeEpochKind;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.StableMemoryRegion;

public class FailOnAllocationStableMemoryBackendTest {
    @Test
    public void failsExactlyTheConfiguredAllocationAndCanBeReset() {
        RecordingBackend delegate = new RecordingBackend();
        FailOnAllocationStableMemoryBackend backend =
                new FailOnAllocationStableMemoryBackend(delegate);
        backend.failOnAllocation(2);

        NativeHandle first = backend.allocate(NativeObjectKind.STRING_BYTES, 8);
        Assert.assertEquals(new NativeHandle(delegate.allocatorId, 8L), first);
        Assert.assertThrows(
                NativeCapacityExceededException.class,
                () -> backend.allocate(NativeObjectKind.STRING_BYTES, 8)
        );
        Assert.assertEquals(2L, backend.allocationAttempts());

        backend.disableFailures();
        backend.allocate(NativeObjectKind.STRING_BYTES, 8);
        Assert.assertEquals(3L, backend.allocationAttempts());
    }

    @Test
    public void onlyCapacityGrowingReallocationConsumesAllocationAttempts() {
        RecordingBackend delegate = new RecordingBackend();
        FailOnAllocationStableMemoryBackend backend =
                new FailOnAllocationStableMemoryBackend(delegate);
        NativeHandle handle = backend.allocate(NativeObjectKind.STRING_BYTES, 8);
        backend.resetAttempts();
        backend.failOnAllocation(1);

        NativeHandle shrunk = backend.reallocate(
                handle,
                4,
                NativeReallocPolicy.PRESERVE_PREFIX
        );
        Assert.assertEquals(0L, backend.allocationAttempts());
        Assert.assertEquals(handle, shrunk);

        NativeHandle sameCapacity = backend.reallocate(
                shrunk,
                8,
                NativeReallocPolicy.PRESERVE_PREFIX
        );
        Assert.assertEquals(0L, backend.allocationAttempts());
        Assert.assertEquals(handle, sameCapacity);

        Assert.assertThrows(
                NativeCapacityExceededException.class,
                () -> backend.reallocate(handle, 32, NativeReallocPolicy.PRESERVE_PREFIX)
        );
        Assert.assertEquals(1L, backend.allocationAttempts());
    }

    @Test
    public void regionFailuresHaveAnIndependentAttemptCounter() {
        RecordingBackend delegate = new RecordingBackend();
        FailOnAllocationStableMemoryBackend backend =
                new FailOnAllocationStableMemoryBackend(delegate);
        backend.failOnAllocation(1L);
        backend.failOnRegionAllocation(2L);

        Assert.assertThrows(
                NativeCapacityExceededException.class,
                () -> backend.allocate(NativeObjectKind.GENERIC, 8)
        );
        Assert.assertSame(delegate.region, backend.allocateRegion("first", 8));
        Assert.assertThrows(
                NativeCapacityExceededException.class,
                () -> backend.allocateRegion("second", 8)
        );
        Assert.assertEquals(1L, backend.allocationAttempts());
        Assert.assertEquals(2L, backend.regionAllocationAttempts());
    }

    @Test
    public void regionFailuresCanBeDisabledAndAttemptsCanBeReset() {
        RecordingBackend delegate = new RecordingBackend();
        FailOnAllocationStableMemoryBackend backend =
                new FailOnAllocationStableMemoryBackend(delegate);
        backend.failOnRegionAllocation(1L);

        Assert.assertThrows(
                NativeCapacityExceededException.class,
                () -> backend.allocateRegion("first", 8)
        );
        Assert.assertEquals(1L, backend.regionAllocationAttempts());

        backend.disableRegionFailures();
        Assert.assertSame(delegate.region, backend.allocateRegion("second", 8));
        Assert.assertEquals(2L, backend.regionAllocationAttempts());

        backend.resetRegionAttempts();
        Assert.assertEquals(0L, backend.regionAllocationAttempts());
    }

    @Test
    public void delegatesEveryNonAllocationOperationExactlyOnce() {
        RecordingBackend delegate = new RecordingBackend();
        FailOnAllocationStableMemoryBackend backend =
                new FailOnAllocationStableMemoryBackend(delegate);
        NativeHandle handle = new NativeHandle(41L, 9L);
        NativeEpochScope epoch = delegate.epoch;
        NativeAllocationScope scope = delegate.scope;
        NativeObjectView view = delegate.view;
        NativeObjectView pinnedView = delegate.pinnedView;
        StableMemoryRegion region = delegate.region;
        MemoryPressureBudget budget = new MemoryPressureBudget(24L, 25L, 26L);
        int[] requests = {33, 34};
        int[] conservativeRequests = {35, 36};

        Assert.assertEquals(delegate.allocatorId, backend.allocatorId());
        backend.bindToCurrentThread();
        backend.free(handle);
        backend.pin(handle);
        backend.unpin(handle);
        Assert.assertSame(epoch, backend.beginEpoch(NativeEpochKind.COMMAND));
        Assert.assertSame(scope, backend.beginAllocationScope());
        Assert.assertEquals(37L, backend.estimateAllocationScopeBookkeepingBytes(38));
        Assert.assertSame(view, backend.resolve(handle, NativeAccessMode.READ_WRITE));
        Assert.assertSame(pinnedView, backend.resolvePinned(handle, NativeAccessMode.READ_ONLY));
        Assert.assertSame(region, backend.allocateRegion("region-owner", 39));
        Assert.assertSame(delegate.defragResult, backend.defragOne(handle, 40L));
        Assert.assertSame(delegate.defragReport, backend.defragCycle(delegate.defragOptions));
        Assert.assertEquals(41L, backend.logicalUsedBytes());
        Assert.assertSame(delegate.stats, backend.stats());
        Assert.assertSame(delegate.metadataStats, backend.metadataStats());
        Assert.assertSame(delegate.usage, backend.memoryUsage());
        Assert.assertSame(delegate.reclaim, backend.trimEmptyPages(budget));
        Assert.assertSame(delegate.growth, backend.estimateAdditionalGrowth(requests));
        Assert.assertSame(
                delegate.growth,
                backend.estimateConservativeAdditionalGrowth(conservativeRequests)
        );
        Assert.assertEquals(42L, backend.liveRegionCount());
        backend.close();

        Assert.assertEquals(
                Arrays.asList(
                        "allocatorId", "bindToCurrentThread", "free", "pin", "unpin", "beginEpoch",
                        "beginAllocationScope", "estimateAllocationScopeBookkeepingBytes", "resolve",
                        "resolvePinned", "allocateRegion", "defragOne", "defragCycle", "logicalUsedBytes",
                        "stats", "metadataStats", "memoryUsage", "trimEmptyPages",
                        "estimateAdditionalGrowth", "estimateConservativeAdditionalGrowth", "liveRegionCount",
                        "close"
                ),
                delegate.operations
        );
        Assert.assertSame(handle, delegate.freedHandle);
        Assert.assertSame(handle, delegate.pinnedHandle);
        Assert.assertSame(handle, delegate.unpinnedHandle);
        Assert.assertEquals(NativeEpochKind.COMMAND, delegate.epochKind);
        Assert.assertEquals(38, delegate.bookkeepingCount);
        Assert.assertSame(handle, delegate.resolvedHandle);
        Assert.assertEquals(NativeAccessMode.READ_WRITE, delegate.resolveMode);
        Assert.assertSame(handle, delegate.pinnedResolvedHandle);
        Assert.assertEquals(NativeAccessMode.READ_ONLY, delegate.resolvePinnedMode);
        Assert.assertEquals("region-owner", delegate.regionOwner);
        Assert.assertEquals(39, delegate.regionBytes);
        Assert.assertSame(handle, delegate.defragHandle);
        Assert.assertEquals(40L, delegate.defragMoveBytes);
        Assert.assertSame(delegate.defragOptions, delegate.seenDefragOptions);
        Assert.assertSame(budget, delegate.seenBudget);
        Assert.assertSame(requests, delegate.seenRequests);
        Assert.assertSame(conservativeRequests, delegate.seenConservativeRequests);
    }

    private static final class RecordingBackend implements StableMemoryBackend {
        private final long allocatorId = 41L;
        private final List<String> operations = new ArrayList<>();
        private final NativeEpochScope epoch = new EpochSentinel();
        private final NativeAllocationScope scope = new AllocationScopeSentinel();
        private final NativeObjectView view = new ViewSentinel(8);
        private final NativeObjectView pinnedView = new ViewSentinel(8);
        private final StableMemoryRegion region = new RegionSentinel();
        private final NativeDefragOptions defragOptions = new NativeDefragOptions(1L, 2L, 3L);
        private final NativeDefragResult defragResult = NativeDefragResult.moved(4L);
        private final NativeDefragReport defragReport = new NativeDefragReport(
                1L, 1L, 4L, 0L, 0L, 0L, false, false, false
        );
        private final NativeAllocatorStats stats = new NativeAllocatorStats(
                1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L,
                9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L
        );
        private final NativeAllocatorMetadataStats metadataStats =
                new NativeAllocatorMetadataStats(17L, 18L);
        private final MemoryUsageSnapshot usage = new MemoryUsageSnapshot(19L, 20L, 21L, 22L, 23L);
        private final MemoryReclaimResult reclaim = new MemoryReclaimResult(
                27L, 28L, 29L, MemoryReclaimResult.StopReason.COMPLETE
        );
        private final NativeAllocationGrowth growth = new NativeAllocationGrowth(30L, 31L, 32L);
        private NativeHandle freedHandle;
        private NativeHandle pinnedHandle;
        private NativeHandle unpinnedHandle;
        private NativeEpochKind epochKind;
        private int bookkeepingCount;
        private NativeHandle resolvedHandle;
        private NativeAccessMode resolveMode;
        private NativeHandle pinnedResolvedHandle;
        private NativeAccessMode resolvePinnedMode;
        private String regionOwner;
        private int regionBytes;
        private NativeHandle defragHandle;
        private long defragMoveBytes;
        private NativeDefragOptions seenDefragOptions;
        private MemoryPressureBudget seenBudget;
        private int[] seenRequests;
        private int[] seenConservativeRequests;

        @Override
        public long allocatorId() {
            operations.add("allocatorId");
            return allocatorId;
        }

        @Override
        public void bindToCurrentThread() {
            operations.add("bindToCurrentThread");
        }

        @Override
        public NativeHandle allocate(NativeObjectKind kind, int size) {
            operations.add("allocate");
            return new NativeHandle(allocatorId, size);
        }

        @Override
        public NativeHandle reallocate(NativeHandle handle, int newSize, NativeReallocPolicy policy) {
            operations.add("reallocate");
            return handle;
        }

        @Override
        public void free(NativeHandle handle) {
            operations.add("free");
            freedHandle = handle;
        }

        @Override
        public void pin(NativeHandle handle) {
            operations.add("pin");
            pinnedHandle = handle;
        }

        @Override
        public void unpin(NativeHandle handle) {
            operations.add("unpin");
            unpinnedHandle = handle;
        }

        @Override
        public NativeEpochScope beginEpoch(NativeEpochKind kind) {
            operations.add("beginEpoch");
            epochKind = kind;
            return epoch;
        }

        @Override
        public NativeAllocationScope beginAllocationScope() {
            operations.add("beginAllocationScope");
            return scope;
        }

        @Override
        public long estimateAllocationScopeBookkeepingBytes(int expectedAllocationCount) {
            operations.add("estimateAllocationScopeBookkeepingBytes");
            bookkeepingCount = expectedAllocationCount;
            return 37L;
        }

        @Override
        public NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode) {
            operations.add("resolve");
            resolvedHandle = handle;
            resolveMode = mode;
            return view;
        }

        @Override
        public NativeObjectView resolvePinned(NativeHandle handle, NativeAccessMode mode) {
            operations.add("resolvePinned");
            pinnedResolvedHandle = handle;
            resolvePinnedMode = mode;
            return pinnedView;
        }

        @Override
        public StableMemoryRegion allocateRegion(String owner, int bytes) {
            operations.add("allocateRegion");
            regionOwner = owner;
            regionBytes = bytes;
            return region;
        }

        @Override
        public NativeDefragResult defragOne(NativeHandle handle, long maxMoveBytes) {
            operations.add("defragOne");
            defragHandle = handle;
            defragMoveBytes = maxMoveBytes;
            return defragResult;
        }

        @Override
        public NativeDefragReport defragCycle(NativeDefragOptions options) {
            operations.add("defragCycle");
            seenDefragOptions = options;
            return defragReport;
        }

        @Override
        public long logicalUsedBytes() {
            operations.add("logicalUsedBytes");
            return 41L;
        }

        @Override
        public NativeAllocatorStats stats() {
            operations.add("stats");
            return stats;
        }

        @Override
        public NativeAllocatorMetadataStats metadataStats() {
            operations.add("metadataStats");
            return metadataStats;
        }

        @Override
        public MemoryUsageSnapshot memoryUsage() {
            operations.add("memoryUsage");
            return usage;
        }

        @Override
        public MemoryReclaimResult trimEmptyPages(MemoryPressureBudget budget) {
            operations.add("trimEmptyPages");
            seenBudget = budget;
            return reclaim;
        }

        @Override
        public NativeAllocationGrowth estimateAdditionalGrowth(int... requestedBytes) {
            operations.add("estimateAdditionalGrowth");
            seenRequests = requestedBytes;
            return growth;
        }

        @Override
        public NativeAllocationGrowth estimateConservativeAdditionalGrowth(int... requestedBytes) {
            operations.add("estimateConservativeAdditionalGrowth");
            seenConservativeRequests = requestedBytes;
            return growth;
        }

        @Override
        public long liveRegionCount() {
            operations.add("liveRegionCount");
            return 42L;
        }

        @Override
        public void close() {
            operations.add("close");
        }

        private static final class EpochSentinel implements NativeEpochScope {
            @Override public NativeEpochKind kind() { return NativeEpochKind.COMMAND; }
            @Override public long epoch() { return 1L; }
            @Override public void close() { }
        }

        private static final class AllocationScopeSentinel implements NativeAllocationScope {
            @Override public NativeAllocationGrowth growth() { return NativeAllocationGrowth.zero(); }
            @Override public void promote() { }
            @Override public void abort() { }
        }

        private static final class ViewSentinel implements NativeObjectView {
            private final int capacity;

            private ViewSentinel(int capacity) {
                this.capacity = capacity;
            }

            @Override public NativeHandle handle() { return NativeHandle.NULL; }
            @Override public int size() { return capacity; }
            @Override public int capacity() { return capacity; }
            @Override public byte getByte(int index) { return 0; }
            @Override public void setByte(int index, byte value) { }
            @Override public void getBytes(int index, byte[] dst, int dstOff, int len) { }
            @Override public void setBytes(int index, byte[] src, int srcOff, int len) { }
            @Override public void close() { }
        }

        private static final class RegionSentinel implements StableMemoryRegion {
            @Override public int size() { return 0; }
            @Override public byte getByte(int offset) { return 0; }
            @Override public void setByte(int offset, byte value) { }
            @Override public int getIntLittleEndian(int offset) { return 0; }
            @Override public void setIntLittleEndian(int offset, int value) { }
            @Override public long getLongLittleEndian(int offset) { return 0L; }
            @Override public void setLongLittleEndian(int offset, long value) { }
            @Override public void getBytes(int offset, byte[] dst, int dstOffset, int length) { }
            @Override public void setBytes(int offset, byte[] src, int srcOffset, int length) { }
            @Override public void copyTo(int sourceOffset, StableMemoryRegion target, int targetOffset, int length) { }
            @Override public void close() { }
        }
    }
}
