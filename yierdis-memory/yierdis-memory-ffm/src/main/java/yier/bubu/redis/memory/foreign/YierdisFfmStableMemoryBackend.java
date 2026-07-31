package yier.bubu.redis.memory.foreign;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocationGrowth;
import yier.bubu.redis.memory.api.NativeAllocationScope;
import yier.bubu.redis.memory.api.NativeAllocationLatencyHistogram;
import yier.bubu.redis.memory.api.NativeAllocatorMetadataStats;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.memory.api.NativeDefragResult;
import yier.bubu.redis.memory.api.NativeEpochKind;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.MemoryOwner;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.NativeObjectKindCounts;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.NativeReallocPolicy;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.StableMemoryRegion;
import yier.bubu.redis.memory.api.StaleNativeHandleException;

public final class YierdisFfmStableMemoryBackend implements StableMemoryBackend {
    private static final int REALLOC_COPY_CHUNK_BYTES = 64 * 1024;
    private static final long ALLOCATION_SCOPE_HEAP_BYTES = 96L;
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long RETIRED_BLOCK_LIST_HEAP_BYTES = 24L;
    private static final long RETIRED_BLOCK_HEAP_BYTES = 96L;
    private static final long[] EMPTY_ALLOCATION_SCOPE_HANDLES = new long[0];
    private static final NativeObjectKind[] OBJECT_KINDS = NativeObjectKind.values();

    private final YierdisNativePageAllocator pageAllocator;
    private final YierdisNativeObjectTable objectTable;
    private final YierdisNativeDefragValidator defragValidator;
    private final long allocatorId;
    private final MemoryOwner owner;
    private final YierdisFfmMemoryRuntime runtime;
    private final AtomicLong externalRegionBytes = new AtomicLong();
    private final List<AllocatorEpochScope> activeEpochScopes = new ArrayList<>();
    private final List<RetiredBlock> retiredBlocks = new ArrayList<>();

    private boolean closed;
    private long currentEpoch;
    private long logicalUsedBytes;
    private long reservedBytes;
    private long liveObjects;
    private long staleHandleDetections;
    private long reallocInPlaceCount;
    private long reallocMovedCount;
    private long defragMovedBytes;
    private long defragSkippedPinnedObjects;
    private long doubleFreeDetections;
    private long defragReclaimedPages;
    private long allocationCount;
    private long allocationTotalNanos;
    private long allocationMaxNanos;
    private long allocationUnder1Micros;
    private long allocationUnder10Micros;
    private long allocationUnder100Micros;
    private long allocationUnder1Millis;
    private long allocationAtLeast1Millis;
    private AllocatorAllocationScope activeAllocationScope;

    public YierdisFfmStableMemoryBackend(String name, int maxSlots, MemoryOwner owner) {
        this(
                new YierdisFfmMemoryRuntime(Objects.requireNonNull(name, "name")),
                maxSlots,
                yier.bubu.redis.memory.api.StableMemoryBackendIds.nextId(),
                owner
        );
    }

    YierdisFfmStableMemoryBackend(
            YierdisFfmMemoryRuntime runtime,
            int maxSlots,
            long allocatorId,
            MemoryOwner owner
    ) {
        this(runtime, maxSlots, allocatorId, owner, (localRaw, sourceMeta, target) -> {
        });
    }

    YierdisFfmStableMemoryBackend(
            YierdisFfmMemoryRuntime runtime,
            int maxSlots,
            long allocatorId,
            MemoryOwner owner,
            YierdisNativeDefragValidator defragValidator
    ) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        if (maxSlots < 0) {
            throw new IllegalArgumentException("maxSlots must be >= 0");
        }
        if (allocatorId <= 0L) {
            throw new IllegalArgumentException("allocatorId must be > 0");
        }
        this.allocatorId = allocatorId;
        this.owner = Objects.requireNonNull(owner, "owner");
        this.defragValidator = Objects.requireNonNull(defragValidator, "defragValidator");
        this.pageAllocator = new YierdisNativePageAllocator(runtime);
        this.objectTable = new YierdisNativeObjectTable(runtime, maxSlots, 0, pageAllocator);
    }

    public long allocatorId() {
        return allocatorId;
    }

    public void bindToCurrentThread() {
        owner.bindToCurrentThread();
    }

    public NativeHandle allocate(NativeObjectKind kind, int size) {
        ensureOpen();
        Objects.requireNonNull(kind, "kind");

        long startedNanos = System.nanoTime();
        YierdisNativeBlock block = pageAllocator.allocate(physicalAllocationBytes(size));
        boolean allocated = false;
        try {
            long localRaw = objectTable.allocate(
                    kind,
                    size,
                    block.capacity(),
                    block.pageId(),
                    block.pageOffset(),
                    block.pageClass().ordinal(),
                    nextEpoch()
            );
            logicalUsedBytes += size;
            reservedBytes += block.capacity();
            liveObjects++;
            allocated = true;
            if (activeAllocationScope != null) {
                try {
                    activeAllocationScope.track(localRaw);
                } catch (RuntimeException | Error failure) {
                    freeLocal(localRaw);
                    throw failure;
                }
            }
            return publicHandle(localRaw);
        } finally {
            recordAllocationLatency(System.nanoTime() - startedNanos);
            if (!allocated) {
                block.close();
            }
        }
    }

    public NativeHandle reallocate(NativeHandle handle, int newSize, NativeReallocPolicy policy) {
        ensureOpen();
        long localRaw = requireOwned(handle);
        return publicHandle(reallocateLocal(localRaw, newSize, policy));
    }

    private long reallocateLocal(long localRaw, int newSize, NativeReallocPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        if (newSize < 0) {
            throw new IllegalArgumentException("newSize must be >= 0");
        }

        YierdisNativeObjectMeta meta = requireLiveMeta(localRaw);
        if (meta.pinCount() > 0) {
            throw new NativeMemoryException("native object is pinned");
        }

        int oldSize = meta.size();
        if (newSize <= meta.capacity()) {
            objectTable.updateLocation(
                    localRaw,
                    newSize,
                    meta.capacity(),
                    meta.segmentId(),
                    meta.address(),
                    meta.pageClass()
            );
            logicalUsedBytes += (long) newSize - oldSize;
            reallocInPlaceCount++;
            return localRaw;
        }

        if (policy == NativeReallocPolicy.NO_MOVE) {
            throw new NativeMemoryException("native object cannot grow in place");
        }

        YierdisNativeBlock previous = pageAllocator.moveSource(meta);
        YierdisNativeBlock next = pageAllocator.allocate(physicalAllocationBytes(newSize));
        boolean moved = false;
        try {
            copyPrefix(previous, next, oldSize);
            objectTable.updateLocation(
                    localRaw,
                    newSize,
                    next.capacity(),
                    next.pageId(),
                    next.pageOffset(),
                    next.pageClass().ordinal()
            );
            logicalUsedBytes += (long) newSize - oldSize;
            reserveMovedBlock(previous, next.capacity(), nextEpoch());
            reallocMovedCount++;
            moved = true;
            if (activeAllocationScope != null) {
                activeAllocationScope.recordGrowth();
            }
            return localRaw;
        } finally {
            if (!moved) {
                next.close();
            }
        }
    }

    public void free(NativeHandle handle) {
        ensureOpen();
        long localRaw = requireOwned(handle);
        freeLocal(localRaw);
    }

    private void freeLocal(long localRaw) {
        if (activeAllocationScope != null) {
            activeAllocationScope.recordGrowth();
        }
        YierdisNativeObjectMeta meta = requireLiveMetaForFree(localRaw);
        long freeEpoch = nextEpoch();
        boolean delayRelease = meta.pinCount() > 0 || !canReclaim(freeEpoch);
        objectTable.free(localRaw, freeEpoch, delayRelease);
        if (delayRelease) {
            reclaimEligibleQuarantine();
            untrackScopedHandle(localRaw);
            return;
        }
        releaseAllocation(meta);
        untrackScopedHandle(localRaw);
    }

    public void pin(NativeHandle handle) {
        ensureOpen();
        long localRaw = requireOwned(handle);
        try {
            objectTable.pin(localRaw);
            objectTable.snapshot(localRaw, false);
        } catch (StaleNativeHandleException e) {
            staleHandleDetections++;
            throw e;
        }
    }

    public void unpin(NativeHandle handle) {
        ensureOpen();
        long localRaw = requireOwned(handle);
        unpinLocal(localRaw);
    }

    private void unpinLocal(long localRaw) {
        YierdisNativeObjectMeta before = objectMeta(localRaw, true);
        objectTable.unpin(localRaw, false);
        if (before.state() == YierdisNativeObjectTable.STATE_FREED_QUARANTINED && before.pinCount() == 1) {
            reclaimEligibleQuarantine();
        }
    }

    public NativeEpochScope beginEpoch(NativeEpochKind kind) {
        ensureOpen();
        AllocatorEpochScope scope = new AllocatorEpochScope(
                Objects.requireNonNull(kind, "kind"),
                nextEpoch()
        );
        activeEpochScopes.add(scope);
        return scope;
    }

    public NativeAllocationScope beginAllocationScope() {
        ensureOpen();
        if (activeAllocationScope != null) {
            throw new IllegalStateException("native allocation scope is already active");
        }
        AllocatorAllocationScope scope = new AllocatorAllocationScope(
                memoryUsage(),
                objectTable.allocationScopeCheckpoint(),
                pageAllocator.beginAllocationScope()
        );
        activeAllocationScope = scope;
        return scope;
    }

    public long estimateAllocationScopeBookkeepingBytes(int expectedAllocationCount) {
        ensureOpen();
        if (expectedAllocationCount < 0) {
            throw new IllegalArgumentException("expectedAllocationCount must be >= 0");
        }
        long heapBytes = MemoryUsageSnapshot.addSaturating(
                ALLOCATION_SCOPE_HEAP_BYTES,
                arrayHeapBytes(allocationScopeHandleCapacity(expectedAllocationCount), Long.BYTES)
        );
        heapBytes = MemoryUsageSnapshot.addSaturating(
                heapBytes,
                objectTable.allocationScopeCheckpointHeapEstimatedBytes()
        );
        return MemoryUsageSnapshot.addSaturating(
                heapBytes,
                pageAllocator.allocationScopeCheckpointHeapEstimatedBytes()
        );
    }

    public NativeObjectView resolve(NativeHandle handle, NativeAccessMode mode) {
        ensureOpen();
        long localRaw = requireOwned(handle);
        Objects.requireNonNull(mode, "mode");
        YierdisNativeObjectMeta meta = requireLiveMeta(localRaw);
        objectTable.pin(localRaw);
        try {
            return new StableObjectView(
                    localRaw,
                    pageAllocator.view(meta),
                    meta.size(),
                    objectTable.resolvedSlot(localRaw, false),
                    mode,
                    true,
                    false
            );
        } catch (RuntimeException e) {
            objectTable.unpin(localRaw);
            throw e;
        }
    }

    public NativeObjectView resolvePinned(NativeHandle handle, NativeAccessMode mode) {
        ensureOpen();
        long localRaw = requireOwned(handle);
        Objects.requireNonNull(mode, "mode");
        if (mode != NativeAccessMode.READ_ONLY) {
            throw new NativeMemoryException("retained native views must be read-only");
        }
        YierdisNativeObjectMeta meta = objectMeta(localRaw, true);
        if (meta.pinCount() <= 0) {
            throw new NativeMemoryException("retained native view requires a pin");
        }
        return new StableObjectView(
                localRaw,
                pageAllocator.view(meta),
                meta.size(),
                objectTable.resolvedSlot(localRaw, true),
                mode,
                false,
                true
        );
    }

    @Override
    public StableMemoryRegion allocateRegion(String regionOwner, int bytes) {
        ensureOpen();
        Objects.requireNonNull(regionOwner, "regionOwner");
        if (bytes <= 0) {
            throw new IllegalArgumentException("bytes must be > 0");
        }
        YierdisFfmRegion region = runtime.allocateRegion(regionOwner, bytes);
        externalRegionBytes.addAndGet(bytes);
        return new TrackingRegion(region, bytes);
    }

    public NativeDefragResult defragOne(NativeHandle handle, long maxMoveBytes) {
        ensureOpen();
        long localRaw = requireOwned(handle);
        if (maxMoveBytes < 0) {
            throw new IllegalArgumentException("maxMoveBytes must be >= 0");
        }
        YierdisNativeObjectMeta meta = requireLiveMeta(localRaw);
        if (meta.size() > maxMoveBytes) {
            return NativeDefragResult.skippedMoveBudget();
        }
        if (meta.pinCount() > 0) {
            defragSkippedPinnedObjects++;
            return NativeDefragResult.skippedPinnedObject();
        }

        return moveLiveObject(localRaw);
    }

    public NativeDefragReport defragCycle(NativeDefragOptions options) {
        ensureOpen();
        Objects.requireNonNull(options, "options");

        YierdisNativeDefragPlanner planner = new YierdisNativeDefragPlanner(options);
        long movedObjects = 0L;
        long skippedPinnedObjects = 0L;
        long skippedBudgetObjects = 0L;
        long failedMoves = 0L;

        for (int slotId = objectTable.firstOccupiedSlot(); slotId != 0;
             slotId = objectTable.nextOccupiedSlot(slotId)) {
            YierdisNativeObjectMeta meta = objectTable.occupiedMeta(slotId);
            if (meta == null) {
                continue;
            }
            if (meta.state() == YierdisNativeObjectTable.STATE_FREED_QUARANTINED) {
                continue;
            }
            if (meta.state() != YierdisNativeObjectTable.STATE_ALLOCATED
                    && meta.state() != YierdisNativeObjectTable.STATE_PINNED) {
                continue;
            }
            if (!planner.canInspectNext()) {
                break;
            }
            planner.onCandidateInspected();

            if (meta.pinCount() > 0) {
                skippedPinnedObjects++;
                defragSkippedPinnedObjects++;
                continue;
            }
            if (!planner.canMove(meta.size())) {
                skippedBudgetObjects++;
                break;
            }

            try {
                long localRaw = localHandleFor(meta);
                NativeDefragResult result = moveLiveObject(localRaw);
                if (result.moved()) {
                    movedObjects++;
                    planner.onMoved(result.movedBytes());
                }
            } catch (RuntimeException e) {
                failedMoves++;
            }
        }

        reclaimEligibleQuarantine();
        return new NativeDefragReport(
                planner.scannedObjects(),
                movedObjects,
                planner.movedBytes(),
                skippedPinnedObjects,
                skippedBudgetObjects,
                failedMoves,
                planner.stoppedByByteBudget(),
                planner.stoppedByObjectBudget(),
                planner.stoppedByTimeBudget()
        );
    }

    public long logicalUsedBytes() {
        ensureOpen();
        return Math.max(0L, logicalUsedBytes);
    }

    public NativeAllocatorStats stats() {
        ensureOpen();
        YierdisNativePageAllocatorStats pageStats = pageAllocator.stats();
        YierdisNativeObjectTableStats tableStats = objectTable.stats();
        return new NativeAllocatorStats(
                logicalUsedBytes,
                reservedBytes,
                pageStats.committedBytes(),
                pageStats.freeBytes(),
                reservedBytes - logicalUsedBytes,
                pageStats.liveSmallPages(),
                pageStats.liveMediumSpanPages(),
                pageStats.liveLargeSpanPages(),
                liveObjects,
                pinnedObjects(),
                quarantinedObjects(),
                staleHandleDetections,
                reallocInPlaceCount,
                reallocMovedCount,
                defragMovedBytes,
                defragSkippedPinnedObjects,
                Math.max(0L, pageStats.freeBytes() - retainedMovedBlockBytes()),
                pageStats.smallFreeBytes(),
                pageStats.mediumFreeBytes(),
                pageStats.largeFreeBytes(),
                pageStats.freePages(),
                quarantineBytes(),
                doubleFreeDetections,
                defragReclaimedPages,
                objectKindCounts(),
                allocationLatencyHistogram(),
                tableStats.metadataCommittedBytes(),
                tableStats.activeSegments(),
                tableStats.freeSlots(),
                tableStats.retiredSlots(),
                tableStats.peakLiveSlots()
        );
    }

    public NativeAllocatorMetadataStats metadataStats() {
        ensureOpen();
        YierdisNativeObjectTableStats tableStats = objectTable.stats();
        return new NativeAllocatorMetadataStats(tableStats.activeSegments(), tableStats.freeSlots());
    }

    public MemoryUsageSnapshot memoryUsage() {
        ensureOpen();
        YierdisNativePageAllocatorStats pageStats = pageAllocator.stats();
        YierdisNativeObjectTableStats tableStats = objectTable.stats();
        long heapBytes = MemoryUsageSnapshot.addSaturating(
                objectTable.heapEstimatedBytes(),
                pageAllocator.heapEstimatedBytes()
        );
        heapBytes = MemoryUsageSnapshot.addSaturating(heapBytes, retiredBlockHeapBytes());
        if (activeAllocationScope != null) {
            heapBytes = MemoryUsageSnapshot.addSaturating(
                    heapBytes,
                    activeAllocationScope.heapEstimatedBytes()
            );
        }
        MemoryUsageSnapshot usage = new MemoryUsageSnapshot(
                heapBytes,
                tableStats.metadataCommittedBytes(),
                pageStats.committedBytes(),
                pageStats.usedBytes(),
                (long) pageStats.emptySmallPages() * YierdisNativePageAllocator.PAGE_BYTES
        );
        long regionBytes = externalRegionBytes.get();
        return usage.plus(new MemoryUsageSnapshot(0L, 0L, regionBytes, regionBytes, 0L));
    }

    @Override
    public long liveRegionCount() {
        owner.checkCurrentThread();
        return runtime.liveRegionCount();
    }

    public MemoryReclaimResult trimEmptyPages(MemoryPressureBudget budget) {
        ensureOpen();
        MemoryReclaimResult result = pageAllocator.trimEmptyPages(budget);
        defragReclaimedPages = MemoryUsageSnapshot.addSaturating(
                defragReclaimedPages,
                result.reclaimedUnits()
        );
        return result;
    }

    public NativeAllocationGrowth estimateAdditionalGrowth(int... requestedBytes) {
        ensureOpen();
        validateRequestedBytes(requestedBytes);
        int additionalSegments = objectTable.estimateAdditionalSegments(requestedBytes.length);
        long metadataBytes = (long) additionalSegments
                * YierdisNativeObjectSegment.SLOTS_PER_SEGMENT
                * YierdisNativeObjectTable.META_BYTES;
        YierdisNativePageAllocator.PageGrowth pageGrowth = pageAllocator.estimateAdditionalGrowth(requestedBytes);
        long heapBytes = MemoryUsageSnapshot.addSaturating(
                objectTable.estimateAdditionalHeapBytes(requestedBytes.length),
                pageGrowth.heapEstimatedBytes()
        );
        return new NativeAllocationGrowth(heapBytes, metadataBytes, pageGrowth.nativeDataCommittedBytes());
    }

    public NativeAllocationGrowth estimateConservativeAdditionalGrowth(int... requestedBytes) {
        ensureOpen();
        validateRequestedBytes(requestedBytes);
        int additionalSegments = objectTable.estimateAdditionalSegments(requestedBytes.length);
        long metadataBytes = (long) additionalSegments
                * YierdisNativeObjectSegment.SLOTS_PER_SEGMENT
                * YierdisNativeObjectTable.META_BYTES;
        YierdisNativePageAllocator.PageGrowth pageGrowth =
                pageAllocator.estimateConservativeAdditionalGrowth(requestedBytes);
        long heapBytes = MemoryUsageSnapshot.addSaturating(
                objectTable.estimateAdditionalHeapBytes(requestedBytes.length),
                pageGrowth.heapEstimatedBytes()
        );
        return new NativeAllocationGrowth(heapBytes, metadataBytes, pageGrowth.nativeDataCommittedBytes());
    }

    public void close() {
        owner.checkCurrentThreadForShutdown();
        RuntimeException failure = null;
        if (!closed) {
            if (activeAllocationScope != null) {
                try {
                    activeAllocationScope.abort();
                } catch (RuntimeException e) {
                    failure = e;
                }
            }
            closed = true;

            long leakedObjects = liveObjects;
            if (leakedObjects > 0L) {
                failure = addFailure(
                        failure,
                        new IllegalStateException("native allocator closed with " + leakedObjects + " live objects")
                );
            }
            long activeEpochs = activeEpochScopes.size();
            if (activeEpochs > 0L) {
                failure = addFailure(
                        failure,
                        new IllegalStateException("native allocator closed with " + activeEpochs + " active epochs")
                );
            }
            retiredBlocks.clear();
            logicalUsedBytes = 0L;
            reservedBytes = 0L;
            liveObjects = 0L;

            try {
                objectTable.close();
            } catch (RuntimeException e) {
                failure = addFailure(failure, e);
            }

            try {
                pageAllocator.close();
            } catch (RuntimeException e) {
                failure = addFailure(failure, e);
            }
        }

        try {
            runtime.close();
        } catch (RuntimeException e) {
            failure = addFailure(failure, e);
        }

        if (failure != null) {
            throw failure;
        }
    }

    YierdisNativeObjectMeta objectMeta(long localRaw, boolean allowQuarantined) {
        ensureOpen();
        try {
            return objectTable.snapshot(localRaw, allowQuarantined);
        } catch (StaleNativeHandleException e) {
            staleHandleDetections++;
            throw e;
        }
    }

    private YierdisNativeObjectMeta requireLiveMeta(long localRaw) {
        try {
            return objectTable.resolve(localRaw);
        } catch (StaleNativeHandleException e) {
            staleHandleDetections++;
            throw e;
        }
    }

    private YierdisNativeObjectMeta requireLiveMetaForFree(long localRaw) {
        try {
            return requireLiveMeta(localRaw);
        } catch (StaleNativeHandleException e) {
            doubleFreeDetections++;
            throw e;
        }
    }

    private static int physicalAllocationBytes(int logicalSize) {
        if (logicalSize < 0) {
            throw new IllegalArgumentException("size must be >= 0");
        }
        return Math.max(1, logicalSize);
    }

    private static void validateRequestedBytes(int... requestedBytes) {
        Objects.requireNonNull(requestedBytes, "requestedBytes");
        for (int requested : requestedBytes) {
            if (requested <= 0) {
                throw new IllegalArgumentException("requestedBytes must contain only positive values");
            }
        }
    }

    private NativeDefragResult moveLiveObject(long localRaw) {
        YierdisNativeObjectMeta sourceMeta = objectTable.beginMove(localRaw);
        YierdisNativeBlock previous = null;
        YierdisNativeBlock target = null;
        boolean published = false;
        try {
            previous = pageAllocator.moveSource(sourceMeta);
            target = pageAllocator.allocate(physicalAllocationBytes(sourceMeta.size()));
            copyPrefix(previous, target, sourceMeta.size());
            defragValidator.validate(localRaw, sourceMeta, target);

            int targetCapacity = target.capacity();
            objectTable.publishMoved(
                    localRaw,
                    sourceMeta.size(),
                    targetCapacity,
                    target.pageId(),
                    target.pageOffset(),
                    target.pageClass().ordinal()
            );
            published = true;
            target = null;
            long retiredBytes = previous.capacity();
            reserveMovedBlock(previous, targetCapacity, nextEpoch());
            defragReclaimedPages += retiredBytes / YierdisNativePageAllocator.PAGE_BYTES;
            defragMovedBytes += sourceMeta.size();
            return NativeDefragResult.moved(sourceMeta.size());
        } catch (RuntimeException e) {
            if (!published) {
                try {
                    objectTable.abortMove(localRaw);
                } catch (RuntimeException abortFailure) {
                    e.addSuppressed(abortFailure);
                }
            }
            throw e;
        } finally {
            if (target != null) {
                target.close();
            }
        }
    }

    private void releaseAllocation(YierdisNativeObjectMeta meta) {
        logicalUsedBytes -= meta.size();
        reservedBytes -= meta.capacity();
        liveObjects--;
        pageAllocator.free(meta);
    }

    private void untrackScopedHandle(long localRaw) {
        if (activeAllocationScope != null) {
            activeAllocationScope.untrack(localRaw);
        }
    }

    private void releaseQuarantinedAllocation(YierdisNativeObjectMeta meta) {
        objectTable.releaseQuarantined(localHandleFor(meta));
        releaseAllocation(meta);
    }

    private void reserveMovedBlock(YierdisNativeBlock block, int nextCapacity, long retiredEpoch) {
        if (canReclaim(retiredEpoch)) {
            block.close();
            reservedBytes += (long) nextCapacity - block.capacity();
            return;
        }
        // 发布新位置后，退役块要保留到所有可能看到旧位置的 scope 关闭。
        reservedBytes += nextCapacity;
        retiredBlocks.add(new RetiredBlock(block, retiredEpoch));
    }

    private void reclaimEligibleQuarantine() {
        reclaimEligibleFreedObjects();
        reclaimEligibleMovedBlocks();
    }

    private void reclaimEligibleFreedObjects() {
        for (int slotId = objectTable.firstOccupiedSlot(); slotId != 0;
             slotId = objectTable.nextOccupiedSlot(slotId)) {
            YierdisNativeObjectMeta meta = objectTable.occupiedMeta(slotId);
            if (meta == null || meta.state() != YierdisNativeObjectTable.STATE_FREED_QUARANTINED) {
                continue;
            }
            if (!canReclaim(meta.freeEpoch())) {
                continue;
            }
            if (meta.pinCount() != 0) {
                continue;
            }
            releaseQuarantinedAllocation(meta);
        }
    }

    private void reclaimEligibleMovedBlocks() {
        Iterator<RetiredBlock> iterator = retiredBlocks.iterator();
        while (iterator.hasNext()) {
            RetiredBlock retired = iterator.next();
            if (!canReclaim(retired.epoch())) {
                continue;
            }
            retired.block().close();
            reservedBytes -= retired.block().capacity();
            iterator.remove();
        }
    }

    private long pinnedObjects() {
        long count = 0L;
        for (int slotId = objectTable.firstOccupiedSlot(); slotId != 0;
             slotId = objectTable.nextOccupiedSlot(slotId)) {
            YierdisNativeObjectMeta meta = objectTable.occupiedMeta(slotId);
            if (meta.pinCount() > 0) {
                count++;
            }
        }
        return count;
    }

    private long quarantinedObjects() {
        long count = 0L;
        for (int slotId = objectTable.firstOccupiedSlot(); slotId != 0;
             slotId = objectTable.nextOccupiedSlot(slotId)) {
            YierdisNativeObjectMeta meta = objectTable.occupiedMeta(slotId);
            if (meta.state() == YierdisNativeObjectTable.STATE_FREED_QUARANTINED) {
                count++;
            }
        }
        return count;
    }

    private long quarantineBytes() {
        long bytes = retainedMovedBlockBytes();
        for (int slotId = objectTable.firstOccupiedSlot(); slotId != 0;
             slotId = objectTable.nextOccupiedSlot(slotId)) {
            YierdisNativeObjectMeta meta = objectTable.occupiedMeta(slotId);
            if (meta.state() == YierdisNativeObjectTable.STATE_FREED_QUARANTINED) {
                bytes += meta.capacity();
            }
        }
        return bytes;
    }

    private long retainedMovedBlockBytes() {
        long bytes = 0L;
        for (RetiredBlock retired : retiredBlocks) {
            bytes += retired.block().capacity();
        }
        return bytes;
    }

    private long retiredBlockHeapBytes() {
        return RETIRED_BLOCK_LIST_HEAP_BYTES + (long) retiredBlocks.size() * RETIRED_BLOCK_HEAP_BYTES;
    }

    private NativeObjectKindCounts objectKindCounts() {
        long generic = 0;
        long stringBytes = 0;
        long listpackBytes = 0;
        long hashFieldBytes = 0;
        long hashValueBytes = 0;
        long setMemberBytes = 0;
        long zsetMemberBytes = 0;
        long scoreBytes = 0;
        long entryRecords = 0;
        long keyBytes = 0;
        long listRoots = 0;
        long hashRoots = 0;
        long setRoots = 0;
        long zsetRoots = 0;
        long listNodes = 0;
        long hashTables = 0;
        long setTables = 0;
        long zsetTables = 0;
        long zsetNodes = 0;
        long indexNodes = 0;
        long metadataRecords = 0;
        for (int slotId = objectTable.firstOccupiedSlot(); slotId != 0;
             slotId = objectTable.nextOccupiedSlot(slotId)) {
            YierdisNativeObjectMeta meta = objectTable.occupiedMeta(slotId);
            if (meta == null || meta.state() == YierdisNativeObjectTable.STATE_FREED_QUARANTINED) {
                continue;
            }
            NativeObjectKind kind = kindFor(meta);
            if (kind == null) {
                continue;
            }
            switch (kind) {
                case GENERIC -> generic++;
                case STRING_BYTES -> stringBytes++;
                case LISTPACK_BYTES -> listpackBytes++;
                case HASH_FIELD_BYTES -> hashFieldBytes++;
                case HASH_VALUE_BYTES -> hashValueBytes++;
                case SET_MEMBER_BYTES -> setMemberBytes++;
                case ZSET_MEMBER_BYTES -> zsetMemberBytes++;
                case SCORE_BYTES -> scoreBytes++;
                case ENTRY_RECORD -> entryRecords++;
                case KEY_BYTES -> keyBytes++;
                case LIST_ROOT -> listRoots++;
                case HASH_ROOT -> hashRoots++;
                case SET_ROOT -> setRoots++;
                case ZSET_ROOT -> zsetRoots++;
                case LIST_NODE -> listNodes++;
                case HASH_TABLE -> hashTables++;
                case SET_TABLE -> setTables++;
                case ZSET_TABLE -> zsetTables++;
                case ZSET_NODE -> zsetNodes++;
                case INDEX_NODE -> indexNodes++;
                case METADATA_RECORD -> metadataRecords++;
            }
        }
        return new NativeObjectKindCounts(
                generic,
                stringBytes,
                listpackBytes,
                hashFieldBytes,
                hashValueBytes,
                setMemberBytes,
                zsetMemberBytes,
                scoreBytes,
                entryRecords,
                keyBytes,
                listRoots,
                hashRoots,
                setRoots,
                zsetRoots,
                listNodes,
                hashTables,
                setTables,
                zsetTables,
                zsetNodes,
                indexNodes,
                metadataRecords
        );
    }

    private NativeAllocationLatencyHistogram allocationLatencyHistogram() {
        return new NativeAllocationLatencyHistogram(
                allocationCount,
                allocationTotalNanos,
                allocationMaxNanos,
                allocationUnder1Micros,
                allocationUnder10Micros,
                allocationUnder100Micros,
                allocationUnder1Millis,
                allocationAtLeast1Millis
        );
    }

    private void recordAllocationLatency(long nanos) {
        long safeNanos = Math.max(0L, nanos);
        allocationCount++;
        allocationTotalNanos += safeNanos;
        allocationMaxNanos = Math.max(allocationMaxNanos, safeNanos);
        if (safeNanos < 1_000L) {
            allocationUnder1Micros++;
        } else if (safeNanos < 10_000L) {
            allocationUnder10Micros++;
        } else if (safeNanos < 100_000L) {
            allocationUnder100Micros++;
        } else if (safeNanos < 1_000_000L) {
            allocationUnder1Millis++;
        } else {
            allocationAtLeast1Millis++;
        }
    }

    private static NativeObjectKind kindFor(YierdisNativeObjectMeta meta) {
        for (NativeObjectKind kind : OBJECT_KINDS) {
            if (kind.domain() == meta.domain() && kind.code() == meta.kindCode()) {
                return kind;
            }
        }
        return null;
    }

    private static long localHandleFor(YierdisNativeObjectMeta meta) {
        NativeObjectKind kind = kindFor(meta);
        if (kind == null) {
            throw new NativeMemoryException("unknown native object kind: " + meta.kindCode());
        }
        return YierdisLocalHandleCodec.encode(
                meta.domain(), kind, meta.slotId(), meta.generation(), meta.flags() & 0x0f
        );
    }

    private long nextEpoch() {
        return ++currentEpoch;
    }

    private boolean canReclaim(long retiredEpoch) {
        if (retiredEpoch <= 0L) {
            return activeEpochScopes.isEmpty();
        }
        // 后启动的 scope 不会引用退役前的位置，因此不应阻塞这次回收。
        return activeEpochScopes.stream().noneMatch(scope -> scope.epoch <= retiredEpoch);
    }

    private static void copyPrefix(YierdisNativeBlock src, YierdisNativeBlock dst, int len) {
        byte[] scratch = new byte[Math.min(len, REALLOC_COPY_CHUNK_BYTES)];
        int offset = 0;
        while (offset < len) {
            int chunk = Math.min(scratch.length, len - offset);
            src.getBytes(offset, scratch, 0, chunk);
            dst.setBytes(offset, scratch, 0, chunk);
            offset += chunk;
        }
    }

    private void ensureOpen() {
        owner.checkCurrentThread();
        if (closed) {
            throw new IllegalStateException("stable native allocator is closed");
        }
    }

    private NativeHandle publicHandle(long localRaw) {
        return localRaw == 0L ? NativeHandle.NULL : new NativeHandle(allocatorId, localRaw);
    }

    private long requireOwned(NativeHandle handle) {
        Objects.requireNonNull(handle, "handle");
        if (handle.allocatorId() != allocatorId) {
            throw new yier.bubu.redis.memory.api.NativeHandleOwnershipException(
                    allocatorId, handle.allocatorId()
            );
        }
        long localRaw = handle.localRaw();
        YierdisLocalHandleCodec.requireValid(localRaw);
        return localRaw;
    }

    private static RuntimeException addFailure(RuntimeException failure, RuntimeException next) {
        if (failure == null) {
            return next;
        }
        failure.addSuppressed(next);
        return failure;
    }

    private final class AllocatorEpochScope implements NativeEpochScope {
        private final NativeEpochKind kind;
        private final long epoch;
        private boolean closedScope;

        private AllocatorEpochScope(NativeEpochKind kind, long epoch) {
            this.kind = kind;
            this.epoch = epoch;
        }

        @Override
        public NativeEpochKind kind() {
            owner.checkCurrentThread();
            return kind;
        }

        @Override
        public long epoch() {
            owner.checkCurrentThread();
            return epoch;
        }

        @Override
        public void close() {
            owner.checkCurrentThread();
            if (closedScope) {
                return;
            }
            closedScope = true;
            if (!activeEpochScopes.remove(this)) {
                throw new IllegalStateException("native epoch is not active: " + epoch);
            }
            if (!closed) {
                reclaimEligibleQuarantine();
            }
        }
    }

    private record RetiredBlock(YierdisNativeBlock block, long epoch) {
        private RetiredBlock {
            Objects.requireNonNull(block, "block");
        }
    }

    private final class AllocatorAllocationScope implements NativeAllocationScope {
        private final MemoryUsageSnapshot baseline;
        private final YierdisNativeObjectTable.AllocationScopeCheckpoint tableCheckpoint;
        private final YierdisNativePageAllocator.AllocationScopeCheckpoint pageCheckpoint;
        private long[] handles = EMPTY_ALLOCATION_SCOPE_HANDLES;
        private int handleCount;
        private NativeAllocationGrowth peakGrowth = NativeAllocationGrowth.zero();
        private boolean terminal;

        private AllocatorAllocationScope(
                MemoryUsageSnapshot baseline,
                YierdisNativeObjectTable.AllocationScopeCheckpoint tableCheckpoint,
                YierdisNativePageAllocator.AllocationScopeCheckpoint pageCheckpoint
        ) {
            this.baseline = baseline;
            this.tableCheckpoint = tableCheckpoint;
            this.pageCheckpoint = pageCheckpoint;
        }

        @Override
        public NativeAllocationGrowth growth() {
            owner.checkCurrentThread();
            if (terminal) {
                return NativeAllocationGrowth.zero();
            }
            recordGrowth();
            return peakGrowth;
        }

        private void recordGrowth() {
            MemoryUsageSnapshot current = memoryUsage();
            NativeAllocationGrowth currentGrowth = new NativeAllocationGrowth(
                    positiveDifference(current.heapEstimatedBytes(), baseline.heapEstimatedBytes()),
                    positiveDifference(current.nativeMetadataCommittedBytes(), baseline.nativeMetadataCommittedBytes()),
                    positiveDifference(
                            current.nativeDataCommittedBytes(),
                            baseline.nativeDataCommittedBytes()
                    )
            );
            peakGrowth = new NativeAllocationGrowth(
                    Math.max(peakGrowth.heapEstimatedBytes(), currentGrowth.heapEstimatedBytes()),
                    Math.max(
                            peakGrowth.nativeMetadataCommittedBytes(),
                            currentGrowth.nativeMetadataCommittedBytes()
                    ),
                    Math.max(
                            peakGrowth.nativeDataCommittedBytes(),
                            currentGrowth.nativeDataCommittedBytes()
                    )
            );
        }

        @Override
        public void promote() {
            owner.checkCurrentThread();
            if (terminal) {
                return;
            }
            pageAllocator.promoteAllocationScope(pageCheckpoint);
            objectTable.promoteAllocationScope(tableCheckpoint);
            terminal = true;
            handleCount = 0;
            handles = EMPTY_ALLOCATION_SCOPE_HANDLES;
            activeAllocationScope = null;
        }

        @Override
        public void abort() {
            owner.checkCurrentThread();
            if (terminal) {
                return;
            }
            pageAllocator.beginAllocationScopeAbort(pageCheckpoint);
            objectTable.beginAllocationScopeAbort(tableCheckpoint);
            terminal = true;
            activeAllocationScope = null;
            RuntimeException failure = null;
            try {
                for (int i = handleCount - 1; i >= 0; i--) {
                    try {
                        freeLocal(handles[i]);
                    } catch (RuntimeException releaseFailure) {
                        failure = addFailure(failure, releaseFailure);
                    }
                }
                try {
                    pageAllocator.restoreAllocationScope(pageCheckpoint);
                } catch (RuntimeException trimFailure) {
                    failure = addFailure(failure, trimFailure);
                }
                try {
                    objectTable.restoreAllocationScopeCheckpoint(tableCheckpoint);
                } catch (RuntimeException trimFailure) {
                    failure = addFailure(failure, trimFailure);
                }
            } finally {
                handleCount = 0;
                handles = EMPTY_ALLOCATION_SCOPE_HANDLES;
            }
            if (failure != null) {
                throw failure;
            }
        }

        @Override
        public void close() {
            owner.checkCurrentThread();
            abort();
        }

        private void track(long localRaw) {
            if (terminal) {
                throw new IllegalStateException("native allocation scope is closed");
            }
            if (handleCount == handles.length) {
                handles = Arrays.copyOf(handles, Math.max(8, handles.length << 1));
            }
            handles[handleCount++] = localRaw;
            recordGrowth();
        }

        private long heapEstimatedBytes() {
            return ALLOCATION_SCOPE_HEAP_BYTES
                    + arrayHeapBytes(handles.length, Long.BYTES)
                    + tableCheckpoint.heapEstimatedBytes()
                    + pageCheckpoint.heapEstimatedBytes();
        }

        private void untrack(long localRaw) {
            for (int i = handleCount - 1; i >= 0; i--) {
                if (handles[i] != localRaw) {
                    continue;
                }
                handles[i] = handles[--handleCount];
                return;
            }
        }

        private long positiveDifference(long current, long before) {
            return current > before ? current - before : 0L;
        }
    }

    private static long arrayHeapBytes(int length, long elementBytes) {
        return ARRAY_HEADER_BYTES + (long) length * elementBytes;
    }

    private static int allocationScopeHandleCapacity(int expectedAllocationCount) {
        int capacity = 0;
        while (capacity < expectedAllocationCount) {
            if (capacity == 0) {
                capacity = 8;
            } else if (capacity > Integer.MAX_VALUE / 2) {
                return Integer.MAX_VALUE;
            } else {
                capacity <<= 1;
            }
        }
        return capacity;
    }

    private final class StableObjectView implements NativeObjectView {
        private final long localRaw;
        private final YierdisNativeBlock block;
        private final int logicalSize;
        private final YierdisNativeObjectTable.ResolvedSlot resolvedSlot;
        private final NativeAccessMode mode;
        private final boolean ownsPin;
        private final boolean allowsQuarantined;
        private boolean closedView;

        private StableObjectView(
                long localRaw,
                YierdisNativeBlock block,
                int logicalSize,
                YierdisNativeObjectTable.ResolvedSlot resolvedSlot,
                NativeAccessMode mode,
                boolean ownsPin,
                boolean allowsQuarantined
        ) {
            this.localRaw = localRaw;
            this.block = block;
            this.logicalSize = logicalSize;
            this.resolvedSlot = resolvedSlot;
            this.mode = mode;
            this.ownsPin = ownsPin;
            this.allowsQuarantined = allowsQuarantined;
        }

        @Override
        public NativeHandle handle() {
            ensureLive();
            return publicHandle(localRaw);
        }

        @Override
        public int size() {
            ensureLive();
            return logicalSize;
        }

        @Override
        public int capacity() {
            ensureLive();
            return block.capacity();
        }

        @Override
        public byte getByte(int index) {
            ensureLive();
            checkRange(index, 1, logicalSize);
            return block.getByte(index);
        }

        @Override
        public void setByte(int index, byte value) {
            ensureWritable();
            checkRange(index, 1, logicalSize);
            block.setByte(index, value);
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            ensureLive();
            checkRange(index, len, logicalSize);
            block.getBytes(index, dst, dstOff, len);
        }

        @Override
        public void setBytes(int index, byte[] src, int srcOff, int len) {
            ensureWritable();
            checkRange(index, len, logicalSize);
            block.setBytes(index, src, srcOff, len);
        }

        @Override
        public void copyBytes(int sourceIndex, int targetIndex, int length) {
            ensureWritable();
            checkRange(sourceIndex, length, logicalSize);
            checkRange(targetIndex, length, logicalSize);
            block.copyBytes(sourceIndex, targetIndex, length);
        }

        @Override
        public boolean contentEquals(int index, byte[] other, int otherOffset, int length) {
            ensureLive();
            if (other == null) {
                throw new IllegalArgumentException("other must not be null");
            }
            if (length < 0) {
                throw new IllegalArgumentException("length must be >= 0");
            }
            if (otherOffset < 0 || otherOffset > other.length - length) {
                throw new IndexOutOfBoundsException();
            }
            checkRange(index, length, logicalSize);
            return block.contentEquals(index, other, otherOffset, length);
        }

        @Override
        public int getIntLittleEndian(int index) {
            ensureLive();
            checkRange(index, Integer.BYTES, logicalSize);
            return block.getIntLittleEndian(index);
        }

        @Override
        public void setIntLittleEndian(int index, int value) {
            ensureWritable();
            checkRange(index, Integer.BYTES, logicalSize);
            block.setIntLittleEndian(index, value);
        }

        @Override
        public long getLongLittleEndian(int index) {
            ensureLive();
            checkRange(index, Long.BYTES, logicalSize);
            return block.getLongLittleEndian(index);
        }

        @Override
        public void setLongLittleEndian(int index, long value) {
            ensureWritable();
            checkRange(index, Long.BYTES, logicalSize);
            block.setLongLittleEndian(index, value);
        }

        @Override
        public void close() {
            owner.checkCurrentThread();
            if (closedView) {
                return;
            }
            closedView = true;
            if (ownsPin && !closed) {
                unpinLocal(localRaw);
            }
        }

        private void ensureWritable() {
            ensureLive();
            if (mode != NativeAccessMode.READ_WRITE) {
                throw new NativeMemoryException("resolved object is read-only");
            }
        }

        private void ensureLive() {
            ensureOpen();
            if (closedView) {
                throw new IllegalStateException("native object view is closed");
            }
            try {
                objectTable.validateResolvedSlot(resolvedSlot, allowsQuarantined);
            } catch (StaleNativeHandleException e) {
                staleHandleDetections++;
                throw e;
            }
        }

        private void checkRange(int index, int len, int size) {
            if (len < 0 || index < 0 || index > size - len) {
                throw new IndexOutOfBoundsException();
            }
        }
    }

    private final class TrackingRegion implements StableMemoryRegion {
        private final YierdisFfmRegion delegate;
        private final int bytes;
        private final AtomicBoolean closedRegion = new AtomicBoolean();

        private TrackingRegion(YierdisFfmRegion delegate, int bytes) {
            this.delegate = delegate;
            this.bytes = bytes;
        }

        @Override
        public int size() {
            checkOpen();
            return delegate.size();
        }

        @Override
        public byte getByte(int offset) {
            checkOpen();
            return delegate.getByte(offset);
        }

        @Override
        public void setByte(int offset, byte value) {
            checkOpen();
            delegate.setByte(offset, value);
        }

        @Override
        public int getIntLittleEndian(int offset) {
            checkOpen();
            return delegate.getIntLittleEndian(offset);
        }

        @Override
        public void setIntLittleEndian(int offset, int value) {
            checkOpen();
            delegate.setIntLittleEndian(offset, value);
        }

        @Override
        public long getLongLittleEndian(int offset) {
            checkOpen();
            return delegate.getLongLittleEndian(offset);
        }

        @Override
        public void setLongLittleEndian(int offset, long value) {
            checkOpen();
            delegate.setLongLittleEndian(offset, value);
        }

        @Override
        public void getBytes(int offset, byte[] dst, int dstOffset, int length) {
            checkOpen();
            delegate.getBytes(offset, dst, dstOffset, length);
        }

        @Override
        public void setBytes(int offset, byte[] src, int srcOffset, int length) {
            checkOpen();
            delegate.setBytes(offset, src, srcOffset, length);
        }

        @Override
        public void copyTo(int sourceOffset, StableMemoryRegion target, int targetOffset, int length) {
            checkOpen();
            Objects.requireNonNull(target, "target");
            if (target instanceof TrackingRegion trackingTarget) {
                trackingTarget.checkOpen();
                delegate.copyTo(sourceOffset, trackingTarget.delegate, targetOffset, length);
                return;
            }
            delegate.copyTo(sourceOffset, target, targetOffset, length);
        }

        @Override
        public void close() {
            owner.checkCurrentThread();
            if (!closedRegion.compareAndSet(false, true)) {
                return;
            }
            delegate.close();
            long remaining = externalRegionBytes.addAndGet(-bytes);
            if (remaining < 0L) {
                throw new IllegalStateException("external region accounting underflow");
            }
        }

        private void checkOpen() {
            owner.checkCurrentThread();
            if (closedRegion.get()) {
                throw new IllegalStateException("stable memory region is closed");
            }
        }
    }

}
